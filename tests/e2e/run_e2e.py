#!/usr/bin/env python3
"""
MWAA Disaster Recovery — End-to-End Test Orchestrator.

Provisions MWAA environments for multiple Airflow versions (configurable),
deploys the DR solution CDK stacks, simulates a disaster, verifies recovery,
and cleans everything up. Reports results as JSON + terminal table, with an
optional Bedrock-generated summary.

Usage:
    ./run_e2e.py                     # full end-to-end run
    ./run_e2e.py --dry-run           # show the plan, touch nothing
    ./run_e2e.py --cleanup-only      # delete all e2e resources and exit
    ./run_e2e.py --versions 2.11.0 3.2.1
    ./run_e2e.py --teardown          # remove a version's resources after PASS
    ./run_e2e.py --sequential        # force sequential even if config says parallel

The script only READS the repository (CDK app, DAG framework); it never
modifies repo code. All test resources are tagged `e2e-framework=<id_prefix>`
for reliable cleanup.
"""

import argparse
import base64
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

try:
    import boto3
    import yaml
    from botocore.exceptions import ClientError, WaiterError  # noqa: F401
except ImportError as e:
    print(f"Missing dependency: {e}. Run: pip install boto3 pyyaml")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

# cdk.json says `python3 app.py`, but npx prepends the Node bin dir to PATH,
# which can resolve `python3` to a different interpreter (e.g. Homebrew's)
# that lacks aws_cdk. Pin the app command to the interpreter running this
# script — the same one the preflight check validates.
CDK_APP = f"{sys.executable} app.py"
TAG_KEY = "e2e-framework"

# Track child processes so Ctrl+C can terminate them (no zombies).
_child_procs: list = []
_child_procs_lock = threading.Lock()


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class Config:
    id_prefix: str = "mwaa-e2e"
    parallel: bool = True
    primary_region: str = "us-east-1"
    secondary_region: str = "us-west-2"
    versions: list = field(default_factory=lambda: ["2.10.3", "2.11.0", "3.0.2", "3.2.1"])
    dr_strategies: list = field(default_factory=lambda: ["WARM_STANDBY"])
    environment_class: str = "mw1.small"
    max_workers: int = 2
    min_workers: int = 1
    bedrock_enabled: bool = True
    bedrock_model_id: str = "us.anthropic.claude-opus-4-20250514-v1:0"
    bedrock_region: str = "us-east-1"
    mwaa_creation_mins: int = 45
    backup_dag_wait_mins: int = 15
    dr_simulation_mins: int = 45
    cdk_deploy_mins: int = 30
    poll_interval_secs: int = 30
    account_id: str = ""

    @classmethod
    def load(cls, path: Path) -> "Config":
        cfg = cls()
        if path.exists():
            raw = yaml.safe_load(path.read_text()) or {}
            tr = raw.get("test_run", {})
            cfg.id_prefix = tr.get("id_prefix", cfg.id_prefix)
            cfg.parallel = bool(tr.get("parallel", cfg.parallel))
            aws = raw.get("aws", {})
            cfg.primary_region = aws.get("primary_region", cfg.primary_region)
            cfg.secondary_region = aws.get("secondary_region", cfg.secondary_region)
            cfg.versions = [str(v) for v in raw.get("versions", cfg.versions)]
            cfg.dr_strategies = raw.get("dr_strategies", cfg.dr_strategies)
            mwaa = raw.get("mwaa", {})
            cfg.environment_class = mwaa.get("environment_class", cfg.environment_class)
            cfg.max_workers = int(mwaa.get("max_workers", cfg.max_workers))
            cfg.min_workers = int(mwaa.get("min_workers", cfg.min_workers))
            br = raw.get("bedrock", {})
            cfg.bedrock_enabled = bool(br.get("enabled", cfg.bedrock_enabled))
            cfg.bedrock_model_id = br.get("model_id", cfg.bedrock_model_id)
            cfg.bedrock_region = br.get("region", cfg.bedrock_region)
            to = raw.get("timeouts", {})
            cfg.mwaa_creation_mins = int(to.get("mwaa_creation_mins", cfg.mwaa_creation_mins))
            cfg.backup_dag_wait_mins = int(to.get("backup_dag_wait_mins", cfg.backup_dag_wait_mins))
            cfg.dr_simulation_mins = int(to.get("dr_simulation_mins", cfg.dr_simulation_mins))
            cfg.cdk_deploy_mins = int(to.get("cdk_deploy_mins", cfg.cdk_deploy_mins))
            cfg.poll_interval_secs = int(to.get("poll_interval_secs", cfg.poll_interval_secs))
        # Account is ALWAYS auto-detected from active credentials.
        cfg.account_id = boto3.client("sts").get_caller_identity()["Account"]
        return cfg


def slug(version: str) -> str:
    return version.replace(".", "-")


# ============================================================================
# Status board — live progress on the main terminal
# ============================================================================

class StatusBoard:
    """Thread-safe status tracking with a background printer.

    Repeated identical status updates (poll loops waiting for a state
    change) are silent: they refresh the timestamp but don't print.
    The background printer only prints when something changed, plus a
    low-frequency heartbeat so long waits still show signs of life.
    """

    HEARTBEAT_SECS = 300  # idle heartbeat while nothing changes

    def __init__(self, log_dir: Path, interval: int = 20):
        self.log_dir = log_dir
        self.interval = interval
        self._lock = threading.Lock()
        self._status: dict = {}       # key -> (status_text, updated_at)
        self._done: dict = {}         # key -> final result str
        self._dirty = False           # something changed since last board print
        self._stop = threading.Event()
        self._start_ts = time.time()
        self.logfile = open(log_dir / "e2e.log", "a")
        self._printer = threading.Thread(target=self._print_loop, daemon=True)

    def log(self, msg: str, key: str = ""):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{ts}]" + (f" [{key}]" if key else "")
        line = f"{prefix} {msg}"
        with self._lock:
            print(line, flush=True)
            self.logfile.write(line + "\n")
            self.logfile.flush()

    def update(self, key: str, status: str):
        with self._lock:
            prev = self._status.get(key)
            unchanged = prev is not None and prev[0] == status
            self._status[key] = (status, time.time())
            if unchanged:
                return  # silent poll — same state as before
            self._dirty = True
        self.log(status, key=key)

    def finish(self, key: str, result: str):
        with self._lock:
            self._done[key] = result
            self._status.pop(key, None)
            self._dirty = True

    def start_printer(self):
        self._printer.start()

    def stop_printer(self):
        self._stop.set()

    @staticmethod
    def _fmt_age(secs: int) -> str:
        return f"{secs // 60}m{secs % 60:02d}s" if secs >= 120 else f"{secs}s"

    def _print_loop(self):
        last_print = time.time()
        while not self._stop.wait(self.interval):
            with self._lock:
                if not self._status:
                    continue
                idle = time.time() - last_print
                if not self._dirty and idle < self.HEARTBEAT_SECS:
                    continue  # nothing new — stay quiet
                self._dirty = False
                last_print = time.time()
                elapsed = int(time.time() - self._start_ts)
                lines = [f"――― progress @ {elapsed // 60}m{elapsed % 60:02d}s ―――"]
                for key, (status, upd) in sorted(self._status.items()):
                    age = int(time.time() - upd)
                    lines.append(f"  ⏳ {key}: {status} ({self._fmt_age(age)} ago)")
                for key, result in sorted(self._done.items()):
                    lines.append(f"  {'✅' if result == 'PASS' else '❌'} {key}: {result}")
                print("\n".join(lines), flush=True)


# ============================================================================
# Subprocess helper (tracked for clean Ctrl+C)
# ============================================================================

def run_cmd(cmd: list, env: dict = None, cwd: Path = None, log_path: Path = None,
            timeout_secs: int = 1800) -> int:
    """Run a subprocess, streaming output to a log file. Returns exit code."""
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    out = open(log_path, "a") if log_path else subprocess.DEVNULL
    proc = subprocess.Popen(cmd, env=full_env, cwd=str(cwd or REPO_ROOT),
                            stdout=out, stderr=subprocess.STDOUT)
    with _child_procs_lock:
        _child_procs.append(proc)
    try:
        return proc.wait(timeout=timeout_secs)
    except subprocess.TimeoutExpired:
        proc.terminate()
        return -1
    finally:
        with _child_procs_lock:
            if proc in _child_procs:
                _child_procs.remove(proc)
        if log_path:
            out.close()


def kill_children():
    with _child_procs_lock:
        for p in _child_procs:
            try:
                p.terminate()
            except Exception:
                pass


# ============================================================================
# Per-version test context
# ============================================================================

@dataclass
class Ctx:
    version: str
    strategy: str
    cfg: Config
    board: StatusBoard
    log_dir: Path
    errors: list = field(default_factory=list)
    checks: dict = field(default_factory=dict)
    timings: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"v{self.version}"

    @property
    def slug(self) -> str:
        return slug(self.version)

    @property
    def stack_prefix(self) -> str:
        return f"{self.cfg.id_prefix}-{self.slug}"

    @property
    def primary_env(self) -> str:
        return f"{self.stack_prefix}-primary"

    @property
    def secondary_env(self) -> str:
        return f"{self.stack_prefix}-secondary"

    @property
    def primary_bucket(self) -> str:
        return f"{self.cfg.id_prefix}-{self.cfg.account_id[:6]}-{self.slug}-pri-dags"

    @property
    def secondary_bucket(self) -> str:
        return f"{self.cfg.id_prefix}-{self.cfg.account_id[:6]}-{self.slug}-sec-dags"

    @property
    def primary_role(self) -> str:
        return f"{self.stack_prefix}-pri-role"

    @property
    def secondary_role(self) -> str:
        return f"{self.stack_prefix}-sec-role"

    def status(self, msg: str):
        self.board.update(self.key, msg)

    def timed(self, name: str, start: float):
        self.timings[name] = round(time.time() - start, 1)


# ============================================================================
# Infrastructure: shared VPCs (one per region), buckets, IAM roles
# ============================================================================

class Infra:
    """Idempotent infrastructure provisioning. All resources tagged for cleanup."""

    def __init__(self, cfg: Config, board: StatusBoard):
        self.cfg = cfg
        self.board = board
        self.vpc_info: dict = {}  # region -> {vpc_id, subnet_ids, sg_id}

    def _tags(self, name: str, rtype: str) -> list:
        return [{
            "ResourceType": rtype,
            "Tags": [{"Key": "Name", "Value": name},
                     {"Key": TAG_KEY, "Value": self.cfg.id_prefix}],
        }]

    @staticmethod
    def _private_egress_ok(ec2, vpc_id: str, subnet_ids: list) -> bool:
        """True if every subnet has an active 0.0.0.0/0 route to an
        available NAT gateway via an explicitly associated route table."""
        try:
            rtbs = ec2.describe_route_tables(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]}])["RouteTables"]
            for subnet_id in subnet_ids:
                rtb = next((r for r in rtbs if any(
                    a.get("SubnetId") == subnet_id for a in r["Associations"])),
                    None)
                if rtb is None:
                    return False
                nat_id = next((rt.get("NatGatewayId") for rt in rtb["Routes"]
                               if rt.get("DestinationCidrBlock") == "0.0.0.0/0"
                               and rt.get("State") == "active"
                               and rt.get("NatGatewayId")), None)
                if not nat_id:
                    return False
                nat = ec2.describe_nat_gateways(
                    NatGatewayIds=[nat_id])["NatGateways"][0]
                if nat["State"] != "available":
                    return False
            return True
        except ClientError:
            return False

    def ensure_shared_vpc(self, region: str, octet: int) -> dict:
        """Create (or find) the shared VPC for a region. Returns network info."""
        ec2 = boto3.client("ec2", region_name=region)
        vpc_name = f"{self.cfg.id_prefix}-shared-vpc"

        # Idempotency: find existing by Name tag
        vpcs = ec2.describe_vpcs(
            Filters=[{"Name": "tag:Name", "Values": [vpc_name]}])["Vpcs"]
        if vpcs:
            vpc_id = vpcs[0]["VpcId"]
            self.board.log(f"Reusing shared VPC {vpc_id} in {region}")
            subnets = ec2.describe_subnets(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:Name", "Values": [f"{vpc_name}-priv-*"]}])["Subnets"]
            sgs = ec2.describe_security_groups(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [f"{vpc_name}-mwaa-sg"]}])["SecurityGroups"]
            if len(subnets) >= 2 and sgs:
                # An interrupted cleanup can leave subnets+SG but delete the
                # route tables / NAT — envs created there have no egress and
                # die with CREATE_FAILED. Only reuse if the private subnets
                # actually route 0.0.0.0/0 to an available NAT gateway.
                subnet_ids = [s["SubnetId"] for s in subnets[:2]]
                if self._private_egress_ok(ec2, vpc_id, subnet_ids):
                    info = {"vpc_id": vpc_id,
                            "subnet_ids": subnet_ids,
                            "sg_id": sgs[0]["GroupId"]}
                    self.vpc_info[region] = info
                    return info
            self.board.log(f"Existing VPC {vpc_id} incomplete "
                           f"(missing routes/NAT/subnets/SG); repairing")

        else:
            self.board.log(f"Creating shared VPC in {region} (10.{octet}.0.0/16)")
            try:
                vpc_id = ec2.create_vpc(
                    CidrBlock=f"10.{octet}.0.0/16",
                    TagSpecifications=self._tags(vpc_name, "vpc"))["Vpc"]["VpcId"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "VpcLimitExceeded":
                    total = len(ec2.describe_vpcs()["Vpcs"])
                    raise RuntimeError(
                        f"VPC limit reached in {region} ({total} VPCs exist). "
                        f"Free up a VPC (leftovers from previous runs? try "
                        f"'./run_e2e.py --cleanup-only') or request a quota "
                        f"increase (Service Quotas → VPC → VPCs per region).")
                raise
            ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_id])
            ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
            ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})

        azs = [z["ZoneName"] for z in ec2.describe_availability_zones(
            Filters=[{"Name": "state", "Values": ["available"]}])["AvailabilityZones"][:2]]

        def ensure_subnet(cidr, az, name):
            existing = ec2.describe_subnets(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:Name", "Values": [name]}])["Subnets"]
            if existing:
                return existing[0]["SubnetId"]
            return ec2.create_subnet(
                VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az,
                TagSpecifications=self._tags(name, "subnet"))["Subnet"]["SubnetId"]

        pub1 = ensure_subnet(f"10.{octet}.1.0/24", azs[0], f"{vpc_name}-pub-1")
        pub2 = ensure_subnet(f"10.{octet}.2.0/24", azs[1], f"{vpc_name}-pub-2")
        priv1 = ensure_subnet(f"10.{octet}.10.0/24", azs[0], f"{vpc_name}-priv-1")
        priv2 = ensure_subnet(f"10.{octet}.11.0/24", azs[1], f"{vpc_name}-priv-2")

        # IGW
        igws = ec2.describe_internet_gateways(Filters=[
            {"Name": "attachment.vpc-id", "Values": [vpc_id]}])["InternetGateways"]
        if igws:
            igw_id = igws[0]["InternetGatewayId"]
        else:
            igw_id = ec2.create_internet_gateway(
                TagSpecifications=self._tags(f"{vpc_name}-igw", "internet-gateway")
            )["InternetGateway"]["InternetGatewayId"]
            ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

        # NAT gateway (in pub1)
        nats = ec2.describe_nat_gateways(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "state", "Values": ["available", "pending"]}])["NatGateways"]
        if nats:
            nat_id = nats[0]["NatGatewayId"]
        else:
            eip = ec2.allocate_address(
                Domain="vpc",
                TagSpecifications=self._tags(f"{vpc_name}-eip", "elastic-ip"))
            nat_id = ec2.create_nat_gateway(
                SubnetId=pub1, AllocationId=eip["AllocationId"],
                TagSpecifications=self._tags(f"{vpc_name}-nat", "natgateway")
            )["NatGateway"]["NatGatewayId"]
        self.board.log(f"Waiting for NAT gateway {nat_id} in {region}...")
        ec2.get_waiter("nat_gateway_available").wait(NatGatewayIds=[nat_id])

        def ensure_rtb(name, target_kwargs, subnet_ids):
            existing = ec2.describe_route_tables(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:Name", "Values": [name]}])["RouteTables"]
            if existing:
                rtb = existing[0]
                rtb_id = rtb["RouteTableId"]
                # Repair path: a surviving route table may have lost its
                # default route, or reference recreated subnets that are no
                # longer associated. Restore both.
                has_default = any(
                    r.get("DestinationCidrBlock") == "0.0.0.0/0"
                    and r.get("State") == "active" for r in rtb["Routes"])
                if not has_default:
                    try:
                        ec2.delete_route(RouteTableId=rtb_id,
                                         DestinationCidrBlock="0.0.0.0/0")
                    except ClientError:
                        pass  # no stale route to remove
                    ec2.create_route(RouteTableId=rtb_id,
                                     DestinationCidrBlock="0.0.0.0/0",
                                     **target_kwargs)
                associated = {a.get("SubnetId") for a in rtb["Associations"]}
                for s in subnet_ids:
                    if s not in associated:
                        ec2.associate_route_table(RouteTableId=rtb_id,
                                                  SubnetId=s)
            else:
                rtb_id = ec2.create_route_table(
                    VpcId=vpc_id, TagSpecifications=self._tags(name, "route-table")
                )["RouteTable"]["RouteTableId"]
                ec2.create_route(RouteTableId=rtb_id,
                                 DestinationCidrBlock="0.0.0.0/0", **target_kwargs)
                for s in subnet_ids:
                    ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=s)
            return rtb_id

        ensure_rtb(f"{vpc_name}-pub-rtb", {"GatewayId": igw_id}, [pub1, pub2])
        ensure_rtb(f"{vpc_name}-priv-rtb", {"NatGatewayId": nat_id}, [priv1, priv2])

        # Security group with self-referencing rule (MWAA requirement)
        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": [f"{vpc_name}-mwaa-sg"]}])["SecurityGroups"]
        if sgs:
            sg_id = sgs[0]["GroupId"]
        else:
            sg_id = ec2.create_security_group(
                GroupName=f"{vpc_name}-mwaa-sg",
                Description="MWAA E2E shared SG", VpcId=vpc_id,
                TagSpecifications=self._tags(f"{vpc_name}-mwaa-sg", "security-group")
            )["GroupId"]
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{"IpProtocol": "-1",
                                "UserIdGroupPairs": [{"GroupId": sg_id}]}])

        info = {"vpc_id": vpc_id, "subnet_ids": [priv1, priv2], "sg_id": sg_id}
        self.vpc_info[region] = info
        self.board.log(f"Shared VPC ready in {region}: {vpc_id}")
        return info

    def ensure_bucket(self, name: str, region: str):
        s3 = boto3.client("s3", region_name=region)
        try:
            s3.head_bucket(Bucket=name)
            self.board.log(f"Reusing bucket {name}")
        except ClientError:
            kwargs = {"Bucket": name}
            if region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**kwargs)
            self.board.log(f"Created bucket {name}")
        s3.put_bucket_versioning(Bucket=name,
                                 VersioningConfiguration={"Status": "Enabled"})
        s3.put_public_access_block(
            Bucket=name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        s3.put_bucket_tagging(Bucket=name, Tagging={
            "TagSet": [{"Key": TAG_KEY, "Value": self.cfg.id_prefix}]})
        # Upload the repo's requirements file for MWAA
        req = REPO_ROOT / "assets" / "requirements.txt"
        if req.exists():
            s3.upload_file(str(req), name, "requirements.txt")
        # Seed the dags/ prefix with an example workload DAG. The DR
        # framework DAGs (backup_metadata etc.) are deployed later by the
        # CDK primary stack's BucketDeployment (prune=False, so this file
        # survives).
        example_dag = SCRIPT_DIR / "assets" / "e2e_example_dag.py"
        if example_dag.exists():
            s3.upload_file(str(example_dag), name, "dags/e2e_example_dag.py")

    def ensure_mwaa_role(self, role_name: str, region: str, bucket: str,
                         env_name: str) -> str:
        iam = boto3.client("iam")
        acct = self.cfg.account_id
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow",
                           "Principal": {"Service": ["airflow.amazonaws.com",
                                                     "airflow-env.amazonaws.com"]},
                           "Action": "sts:AssumeRole"}]})
        try:
            arn = iam.create_role(
                RoleName=role_name, AssumeRolePolicyDocument=trust,
                Tags=[{"Key": TAG_KEY, "Value": self.cfg.id_prefix}])["Role"]["Arn"]
            self.board.log(f"Created role {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityAlreadyExists":
                raise
            arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            self.board.log(f"Reusing role {role_name}")

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "airflow:PublishMetrics",
                 "Resource": f"arn:aws:airflow:{region}:{acct}:environment/{env_name}"},
                {"Effect": "Allow",
                 "Action": ["s3:GetObject*", "s3:GetBucket*", "s3:List*",
                            "s3:PutObject*", "s3:DeleteObject*"],
                 "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*",
                              f"arn:aws:s3:::{self.cfg.id_prefix}-*"]},
                {"Effect": "Allow",
                 "Action": ["logs:CreateLogStream", "logs:CreateLogGroup",
                            "logs:PutLogEvents", "logs:GetLogEvents",
                            "logs:GetLogRecord", "logs:GetLogGroupFields",
                            "logs:GetQueryResults"],
                 "Resource": [f"arn:aws:logs:{region}:{acct}:log-group:airflow-*"]},
                {"Effect": "Allow",
                 "Action": ["logs:DescribeLogGroups", "cloudwatch:PutMetricData",
                            "s3:GetAccountPublicAccessBlock"],
                 "Resource": ["*"]},
                {"Effect": "Allow",
                 "Action": ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
                            "sqs:ReceiveMessage", "sqs:SendMessage"],
                 "Resource": f"arn:aws:sqs:{region}:*:airflow-celery-*"},
                {"Effect": "Allow",
                 "Action": ["kms:Decrypt", "kms:DescribeKey",
                            "kms:GenerateDataKey*", "kms:Encrypt"],
                 "NotResource": f"arn:aws:kms:*:{acct}:key/*",
                 "Condition": {"StringLike": {
                     "kms:ViaService": [f"sqs.{region}.amazonaws.com"]}}},
            ],
        }
        iam.put_role_policy(RoleName=role_name, PolicyName="mwaa-exec",
                            PolicyDocument=json.dumps(policy))
        return arn

    def provision_for_version(self, ctx: Ctx):
        t0 = time.time()
        ctx.status("infra: buckets + roles")
        self.ensure_bucket(ctx.primary_bucket, self.cfg.primary_region)
        self.ensure_bucket(ctx.secondary_bucket, self.cfg.secondary_region)
        ctx.pri_role_arn = self.ensure_mwaa_role(
            ctx.primary_role, self.cfg.primary_region,
            ctx.primary_bucket, ctx.primary_env)
        ctx.sec_role_arn = self.ensure_mwaa_role(
            ctx.secondary_role, self.cfg.secondary_region,
            ctx.secondary_bucket, ctx.secondary_env)
        # IAM eventual consistency
        time.sleep(10)
        ctx.timed("infra", t0)


# ============================================================================
# MWAA environment lifecycle
# ============================================================================

def create_mwaa_env(ctx: Ctx, env_name: str, region: str, bucket: str,
                    role_arn: str, net: dict):
    mwaa = boto3.client("mwaa", region_name=region)
    try:
        status = mwaa.get_environment(Name=env_name)["Environment"]["Status"]
        if status == "CREATE_FAILED":
            # Resume after a failed run: a CREATE_FAILED env can only be
            # deleted, so remove it and recreate below.
            ctx.board.log(f"MWAA env {env_name} is CREATE_FAILED — deleting "
                          f"before recreate", key=ctx.key)
            delete_mwaa_env(ctx.cfg, ctx.board, env_name, region)
        else:
            ctx.board.log(f"MWAA env {env_name} already exists "
                          f"({status}), reusing")
            return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    ctx.board.log(f"Creating MWAA env {env_name} (v{ctx.version}) in {region}")
    mwaa.create_environment(
        Name=env_name,
        AirflowVersion=ctx.version,
        SourceBucketArn=f"arn:aws:s3:::{bucket}",
        DagS3Path="dags",
        RequirementsS3Path="requirements.txt",
        ExecutionRoleArn=role_arn,
        EnvironmentClass=ctx.cfg.environment_class,
        MaxWorkers=ctx.cfg.max_workers,
        MinWorkers=ctx.cfg.min_workers,
        WebserverAccessMode="PUBLIC_ONLY",
        NetworkConfiguration={
            "SubnetIds": net["subnet_ids"],
            "SecurityGroupIds": [net["sg_id"]],
        },
        LoggingConfiguration={
            "DagProcessingLogs": {"Enabled": True, "LogLevel": "INFO"},
            "SchedulerLogs": {"Enabled": True, "LogLevel": "INFO"},
            "TaskLogs": {"Enabled": True, "LogLevel": "INFO"},
            "WebserverLogs": {"Enabled": True, "LogLevel": "INFO"},
            "WorkerLogs": {"Enabled": True, "LogLevel": "INFO"},
        },
        Tags={TAG_KEY: ctx.cfg.id_prefix},
    )


def wait_mwaa_available(ctx: Ctx, targets: list) -> bool:
    """Wait for MWAA environments to become AVAILABLE.

    targets: list of (env_name, region). Both environments are created
    up-front (CreateEnvironment is async), so this polls all of them in a
    single loop and reports a combined status.
    """
    clients: dict = {}
    pending = dict(targets)  # env_name -> region
    deadline = time.time() + ctx.cfg.mwaa_creation_mins * 60
    ok = True
    while pending and time.time() < deadline:
        parts = []
        for env_name, region in list(pending.items()):
            mwaa = clients.setdefault(
                region, boto3.client("mwaa", region_name=region))
            try:
                env = mwaa.get_environment(Name=env_name)["Environment"]
                status = env["Status"]
            except ClientError:
                env, status = {}, "UNKNOWN"
            if status == "AVAILABLE":
                pending.pop(env_name)
                ctx.board.log(f"MWAA {env_name} AVAILABLE", key=ctx.key)
                continue
            if status in ("CREATE_FAILED", "UNAVAILABLE"):
                # Capture WHY before cleanup deletes the evidence
                reason = (env.get("LastUpdate", {}).get("Error", {})
                          .get("ErrorMessage", "no error message provided"))
                dump = ctx.log_dir / f"mwaa_failed_{env_name}.json"
                dump.write_text(json.dumps(env, indent=2, default=str))
                ctx.errors.append(f"{env_name} reached status {status}: {reason}")
                ctx.board.log(f"MWAA {env_name} {status}: {reason} "
                              f"(details: {dump.name})", key=ctx.key)
                pending.pop(env_name)
                ok = False
                continue
            parts.append(f"{env_name}: {status}")
        if parts:
            ctx.status("MWAA " + " | ".join(sorted(parts)))
            time.sleep(60)
    for env_name in pending:
        ctx.errors.append(f"{env_name} did not become AVAILABLE in time")
    return ok and not pending


def mwaa_env_status(env_name: str, region: str) -> str:
    """Return the MWAA environment status, or 'ABSENT' if it doesn't exist."""
    try:
        mwaa = boto3.client("mwaa", region_name=region)
        return mwaa.get_environment(Name=env_name)["Environment"]["Status"]
    except ClientError:
        return "ABSENT"


def infra_ready(ctx: Ctx) -> bool:
    """True if both MWAA environments for this version exist and are AVAILABLE."""
    return (mwaa_env_status(ctx.primary_env, ctx.cfg.primary_region) == "AVAILABLE"
            and mwaa_env_status(ctx.secondary_env, ctx.cfg.secondary_region) == "AVAILABLE")


def adopt_existing_infra(ctx: Ctx):
    """Populate ctx with details of already-provisioned infra (reuse mode)."""
    iam = boto3.client("iam")
    ctx.pri_role_arn = iam.get_role(RoleName=ctx.primary_role)["Role"]["Arn"]
    ctx.sec_role_arn = iam.get_role(RoleName=ctx.secondary_role)["Role"]["Arn"]
    # Make sure the example DAG is present (buckets aren't re-provisioned)
    example_dag = SCRIPT_DIR / "assets" / "e2e_example_dag.py"
    if example_dag.exists():
        for bucket, region in ((ctx.primary_bucket, ctx.cfg.primary_region),
                               (ctx.secondary_bucket, ctx.cfg.secondary_region)):
            boto3.client("s3", region_name=region).upload_file(
                str(example_dag), bucket, "dags/e2e_example_dag.py")


def deploy_mwaa(ctx: Ctx, infra: Infra):
    t0 = time.time()
    cfg = ctx.cfg
    create_mwaa_env(ctx, ctx.primary_env, cfg.primary_region,
                    ctx.primary_bucket, ctx.pri_role_arn,
                    infra.vpc_info[cfg.primary_region])
    create_mwaa_env(ctx, ctx.secondary_env, cfg.secondary_region,
                    ctx.secondary_bucket, ctx.sec_role_arn,
                    infra.vpc_info[cfg.secondary_region])
    ctx.status("waiting for MWAA envs (20-40 min)...")
    ok = wait_mwaa_available(ctx, [
        (ctx.primary_env, cfg.primary_region),
        (ctx.secondary_env, cfg.secondary_region),
    ])
    ctx.timed("mwaa_create", t0)
    if not ok:
        raise RuntimeError(f"MWAA creation failed: {ctx.errors}")


# ============================================================================
# CDK deployment of the DR solution
# ============================================================================

def build_env_vars(ctx: Ctx, infra: Infra, simulate: bool) -> dict:
    cfg = ctx.cfg
    pnet = infra.vpc_info[cfg.primary_region]
    snet = infra.vpc_info[cfg.secondary_region]
    return {
        "STACK_NAME_PREFIX": ctx.stack_prefix,
        "AWS_ACCOUNT_ID": cfg.account_id,
        "DR_TYPE": ctx.strategy,
        "MWAA_VERSION": ctx.version,
        "MWAA_UPDATE_EXECUTION_ROLE": "YES",
        "MWAA_SIMULATE_DR": "YES" if simulate else "NO",
        "PRIMARY_REGION": cfg.primary_region,
        "PRIMARY_MWAA_ENVIRONMENT_NAME": ctx.primary_env,
        "PRIMARY_MWAA_ROLE_ARN": ctx.pri_role_arn,
        "PRIMARY_DAGS_BUCKET_NAME": ctx.primary_bucket,
        "PRIMARY_VPC_ID": pnet["vpc_id"],
        "PRIMARY_SUBNET_IDS": json.dumps(pnet["subnet_ids"]),
        "PRIMARY_SECURITY_GROUP_IDS": json.dumps([pnet["sg_id"]]),
        "PRIMARY_BACKUP_SCHEDULE": "0 * * * *",
        "SECONDARY_REGION": cfg.secondary_region,
        "SECONDARY_MWAA_ENVIRONMENT_NAME": ctx.secondary_env,
        "SECONDARY_MWAA_ROLE_ARN": ctx.sec_role_arn,
        "SECONDARY_DAGS_BUCKET_NAME": ctx.secondary_bucket,
        "SECONDARY_VPC_ID": snet["vpc_id"],
        "SECONDARY_SUBNET_IDS": json.dumps(snet["subnet_ids"]),
        "SECONDARY_SECURITY_GROUP_IDS": json.dumps([snet["sg_id"]]),
        "SECONDARY_CREATE_SFN_VPCE": "NO",
        "HEALTH_CHECK_ENABLED": "NO",  # we trigger recovery manually
    }


def cdk_deploy(ctx: Ctx, infra: Infra, simulate: bool = False):
    t0 = time.time()
    ctx.status(f"CDK deploy (simulate={simulate})")
    env = build_env_vars(ctx, infra, simulate)
    log = ctx.log_dir / f"cdk_deploy_{ctx.slug}.log"
    # Each version needs its own cdk.out to avoid clashes in parallel runs
    rc = run_cmd(
        ["npx", "cdk", "deploy", "--all", "--require-approval", "never",
         "--app", CDK_APP,
         "--output", f"cdk.out.e2e-{ctx.slug}"],
        env=env, cwd=REPO_ROOT, log_path=log,
        timeout_secs=ctx.cfg.cdk_deploy_mins * 60)
    ctx.timed("cdk_deploy" + ("_sim" if simulate else ""), t0)
    if rc != 0:
        raise RuntimeError(f"cdk deploy failed (rc={rc}), see {log}")


def cdk_destroy(ctx: Ctx, infra: Infra):
    ctx.status("CDK destroy")
    env = build_env_vars(ctx, infra, simulate=False)
    log = ctx.log_dir / f"cdk_destroy_{ctx.slug}.log"
    run_cmd(["npx", "cdk", "destroy", "--all", "--force",
             "--app", CDK_APP,
             "--output", f"cdk.out.e2e-{ctx.slug}"],
            env=env, cwd=REPO_ROOT, log_path=log,
            timeout_secs=ctx.cfg.cdk_deploy_mins * 60)


# ============================================================================
# Airflow interaction via the MWAA CLI token API
# ============================================================================

def airflow_cli(env_name: str, region: str, command: str) -> tuple:
    """Run an Airflow CLI command via MWAA's CLI token endpoint (Airflow 2.x
    only — the /aws_mwaa/cli endpoint does not exist on Airflow 3 envs).
    Returns (stdout, stderr)."""
    mwaa = boto3.client("mwaa", region_name=region)
    tok = mwaa.create_cli_token(Name=env_name)
    req = urllib.request.Request(
        f"https://{tok['WebServerHostname']}/aws_mwaa/cli",
        data=command.encode(),
        headers={"Authorization": f"Bearer {tok['CliToken']}",
                 "Content-Type": "text/plain"},
        method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read())
    out = base64.b64decode(body.get("stdout", "")).decode()
    err = base64.b64decode(body.get("stderr", "")).decode()
    return out, err


def _is_airflow3(version: str) -> bool:
    return version.split(".")[0] == "3"


def _rest_api(env_name: str, region: str, method: str, path: str,
              body: dict = None, query: dict = None) -> dict:
    """Call the Airflow stable REST API through MWAA InvokeRestApi."""
    mwaa = boto3.client("mwaa", region_name=region)
    kwargs = {"Name": env_name, "Method": method, "Path": path}
    if body is not None:
        kwargs["Body"] = body
    if query is not None:
        kwargs["QueryParameters"] = query
    return mwaa.invoke_rest_api(**kwargs)


def airflow_set_variable(ctx: Ctx, env_name: str, region: str,
                         key: str, value: str):
    if _is_airflow3(ctx.version):
        # POST fails if the variable already exists (leftover from a
        # previous run) — try PATCH first, fall back to POST.
        try:
            _rest_api(env_name, region, "PATCH", f"/variables/{key}",
                      body={"key": key, "value": value})
        except ClientError:
            _rest_api(env_name, region, "POST", "/variables",
                      body={"key": key, "value": value})
    else:
        airflow_cli(env_name, region, f"variables set {key} {value}")


def airflow_get_variable(ctx: Ctx, env_name: str, region: str,
                         key: str) -> str:
    if _is_airflow3(ctx.version):
        resp = _rest_api(env_name, region, "GET", f"/variables/{key}")
        return str(resp.get("RestApiResponse", {}).get("value", ""))
    out, _ = airflow_cli(env_name, region, f"variables get {key}")
    return out


def airflow_delete_variable(ctx: Ctx, env_name: str, region: str, key: str):
    """Delete a variable; missing keys are fine."""
    try:
        if _is_airflow3(ctx.version):
            _rest_api(env_name, region, "DELETE", f"/variables/{key}")
        else:
            airflow_cli(env_name, region, f"variables delete {key}")
    except Exception:
        pass  # variable didn't exist — nothing to clear


def airflow_unpause_and_trigger(ctx: Ctx, env_name: str, region: str,
                                dag_id: str):
    if _is_airflow3(ctx.version):
        _rest_api(env_name, region, "PATCH", f"/dags/{dag_id}",
                  body={"is_paused": False},
                  query={"update_mask": "is_paused"})
        _rest_api(env_name, region, "POST", f"/dags/{dag_id}/dagRuns",
                  body={"logical_date": None})
    else:
        airflow_cli(env_name, region, f"dags unpause {dag_id}")
        airflow_cli(env_name, region, f"dags trigger {dag_id}")


def airflow_dag_run_states(ctx: Ctx, env_name: str, region: str,
                           dag_id: str, limit: int = 5) -> list:
    """Return recent DAG run states (newest first), e.g. ['running', 'failed']."""
    if _is_airflow3(ctx.version):
        resp = _rest_api(env_name, region, "GET", f"/dags/{dag_id}/dagRuns",
                         query={"limit": str(limit), "order_by": "-run_after"})
        runs = resp.get("RestApiResponse", {}).get("dag_runs", [])
        return [r.get("state") for r in runs]
    out, _ = airflow_cli(env_name, region,
                         f"dags list-runs -d {dag_id} -o json")
    try:
        start = out.index("[")
        runs = json.loads(out[start:])
    except (ValueError, json.JSONDecodeError):
        return []
    runs.sort(key=lambda r: r.get("execution_date") or
              r.get("logical_date") or "", reverse=True)
    return [r.get("state") for r in runs[:limit]]


# ============================================================================
# Test execution: seed → backup → simulate DR → verify
# ============================================================================

MARKER_VAR = "e2e_dr_marker"


def seed_test_data(ctx: Ctx):
    """Create a marker Airflow variable in the primary env to verify after DR."""
    marker = f"e2e-{ctx.slug}-{int(time.time())}"
    try:
        airflow_set_variable(ctx, ctx.primary_env, ctx.cfg.primary_region,
                             MARKER_VAR, marker)
        # The DR restore strategy is APPEND: existing variables are NOT
        # overwritten. A leftover marker in the secondary env (previous
        # test on reused infra) would mask the restore — remove it so the
        # restored value can only come from this run's backup.
        airflow_delete_variable(ctx, ctx.secondary_env,
                                ctx.cfg.secondary_region, MARKER_VAR)
        ctx.marker = marker
        ctx.checks["seed_marker"] = "PASS"
        ctx.board.log(f"Seeded marker variable = {marker}", key=ctx.key)
    except Exception as e:
        ctx.marker = None
        ctx.checks["seed_marker"] = f"SKIP ({e})"


def find_backup_bucket(ctx: Ctx, region: str, stack_suffix: str) -> str:
    """Find the backup bucket created by a DR stack via CloudFormation resources."""
    cfn = boto3.client("cloudformation", region_name=region)
    stack = f"{ctx.stack_prefix}-{stack_suffix}"
    paginator = cfn.get_paginator("list_stack_resources")
    for page in paginator.paginate(StackName=stack):
        for res in page["StackResourceSummaries"]:
            if res["ResourceType"] == "AWS::S3::Bucket" and \
                    "backup" in res["LogicalResourceId"].lower():
                return res["PhysicalResourceId"]
    raise RuntimeError(f"No backup bucket found in stack {stack}")


# The DR solution's Glue job names are NOT namespaced per deployment
# (backup_metadata_export, restore_metadata_import, cleanup_metadata_cleanup),
# so parallel version tests in the same account+region share the same Glue
# jobs (max concurrency 1) and clash with ConcurrentRunsExceeded. Serialize
# the backup and DR-simulation phases across version threads. This is purely
# an e2e concern — a real deployment is one per account/region.
_backup_phase_lock = threading.Lock()
_dr_phase_lock = threading.Lock()


def _newest_object_ts(s3, bucket: str, prefix: str):
    """Newest LastModified under a prefix, or None if empty."""
    newest = None
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if newest is None or obj["LastModified"] > newest:
                newest = obj["LastModified"]
    return newest


def trigger_and_wait_backup(ctx: Ctx):
    """Serialized wrapper: see _backup_phase_lock/_dr_phase_lock note."""
    if not _backup_phase_lock.acquire(blocking=False):
        ctx.status("waiting for backup slot (serialized across versions "
                   "— shared Glue jobs)...")
        _backup_phase_lock.acquire()
    try:
        _trigger_and_wait_backup_impl(ctx)
    finally:
        _backup_phase_lock.release()


def _trigger_and_wait_backup_impl(ctx: Ctx):
    """Trigger the backup DAG and wait for FRESH backup data in S3.

    Freshness: stale CSVs from a previous run must not satisfy the check
    (they would restore outdated state), so we baseline the newest object
    timestamp in both backup buckets before triggering and only accept
    objects newer than the baseline.

    Concurrency-aware: backup_metadata also runs on its own hourly schedule
    and both point at one Glue job (max concurrency 1), so a blind trigger
    can fail with ConcurrentRunsExceeded. If a run is already active we
    just wait for it; if the latest run failed we retrigger with backoff.
    """
    t0 = time.time()
    ctx.status("triggering backup_metadata DAG")
    env, region = ctx.primary_env, ctx.cfg.primary_region

    # Freshness baseline: newest pre-existing object in the primary bucket
    bucket = find_backup_bucket(ctx, region, "primary-stack")
    s3 = boto3.client("s3", region_name=region)
    baseline = _newest_object_ts(s3, bucket, "data/")
    if baseline:
        ctx.board.log(f"Stale backup data present (newest {baseline:%H:%M:%S}) "
                      f"— waiting for FRESH objects only", key=ctx.key)

    def run_states() -> list:
        try:
            return airflow_dag_run_states(ctx, env, region, "backup_metadata")
        except Exception:
            return []

    def has_active(states) -> bool:
        return any(s in ("queued", "running") for s in states)

    try:
        if has_active(run_states()):
            ctx.board.log("backup_metadata run already active — waiting for it "
                          "instead of triggering (avoids Glue concurrency clash)",
                          key=ctx.key)
        else:
            airflow_unpause_and_trigger(ctx, env, region, "backup_metadata")
    except Exception as e:
        ctx.board.log(f"DAG trigger failed ({e}); relying on schedule",
                      key=ctx.key)

    deadline = time.time() + ctx.cfg.backup_dag_wait_mins * 60
    retriggers_left = 3
    last_trigger = time.time()
    while time.time() < deadline:
        newest = _newest_object_ts(s3, bucket, "data/")
        fresh = newest is not None and (baseline is None or newest > baseline)
        states = run_states()
        # PASS only when the backup run COMPLETED and fresh data exists —
        # the DAG writes many table files over minutes; passing on the
        # first fresh object lets DR restore a half-written/stale set.
        if fresh and states and states[0] == "success":
            ctx.checks["backup_created"] = "PASS"
            ctx.timed("backup", t0)
            ctx.board.log(f"Backup run succeeded; fresh data in "
                          f"s3://{bucket}/data/", key=ctx.key)
            return
        # If nothing is active and no fresh data materialized, retrigger
        # (bounded, spaced out — covers failed runs and lost triggers).
        if (not fresh and states and not has_active(states)
                and retriggers_left > 0
                and time.time() - last_trigger > 120):
            retriggers_left -= 1
            ctx.board.log(f"no active backup run and no fresh data — "
                          f"retriggering ({retriggers_left} retries left)",
                          key=ctx.key)
            try:
                airflow_unpause_and_trigger(ctx, env, region, "backup_metadata")
            except Exception as e:
                ctx.board.log(f"retrigger failed ({e})", key=ctx.key)
            last_trigger = time.time()
        ctx.status("waiting for backup run to complete...")
        time.sleep(30)
    ctx.checks["backup_created"] = "FAIL"
    raise RuntimeError("Backup run did not complete with fresh data in time")


def _list_data_objects(s3, bucket: str) -> dict:
    """Map of key -> (LastModified, Size) under data/."""
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="data/"):
        for o in page.get("Contents", []):
            out[o["Key"]] = (o["LastModified"], o["Size"])
    return out


def wait_replication(ctx: Ctx):
    """Wait until EVERY backup object is replicated to the secondary bucket.

    Passing on the first fresh replica is not enough: DR restore reads
    specific files (e.g. variable.csv), so a partially replicated set makes
    it restore stale data. Requires each primary data/ object to exist in
    the secondary with the same size and a timestamp that is not older.
    """
    p_bucket = find_backup_bucket(ctx, ctx.cfg.primary_region, "primary-stack")
    s_bucket = find_backup_bucket(ctx, ctx.cfg.secondary_region,
                                  "secondary-stack")
    s3p = boto3.client("s3", region_name=ctx.cfg.primary_region)
    s3s = boto3.client("s3", region_name=ctx.cfg.secondary_region)
    deadline = time.time() + 10 * 60
    while time.time() < deadline:
        pk = _list_data_objects(s3p, p_bucket)
        sk = _list_data_objects(s3s, s_bucket)
        pending = [k for k, (lm, size) in pk.items()
                   if k not in sk or sk[k][1] != size or sk[k][0] < lm]
        if pk and not pending:
            ctx.checks["replication"] = "PASS"
            ctx.board.log(f"All {len(pk)} backup objects replicated",
                          key=ctx.key)
            return
        ctx.status(f"waiting for replication ({len(pending)}/{len(pk)} "
                   f"objects pending)...")
        time.sleep(20)
    ctx.checks["replication"] = "FAIL"
    raise RuntimeError("Backup data never fully replicated to secondary region")


def simulate_dr(ctx: Ctx):
    """Serialized wrapper: see _backup_phase_lock/_dr_phase_lock note."""
    if not _dr_phase_lock.acquire(blocking=False):
        ctx.status("waiting for DR simulation slot (serialized across versions "
                   "— shared Glue jobs)...")
        _dr_phase_lock.acquire()
    try:
        _simulate_dr_impl(ctx)
    finally:
        _dr_phase_lock.release()


def _simulate_dr_impl(ctx: Ctx):
    """Manually start the recovery StepFunctions with simulate_dr=YES
    (documented manual-trigger method) and wait for completion."""
    t0 = time.time()
    sfn = boto3.client("stepfunctions", region_name=ctx.cfg.secondary_region)
    # CloudFormation strips hyphens from CDK construct ids, so the deployed
    # name looks like 'mwaae2e2103statemachine<hash>' — resolve it from the
    # secondary stack's resources instead of matching on a name prefix.
    cfn = boto3.client("cloudformation", region_name=ctx.cfg.secondary_region)
    stack = f"{ctx.stack_prefix}-secondary-stack"
    arn = None
    paginator = cfn.get_paginator("list_stack_resources")
    for page in paginator.paginate(StackName=stack):
        for res in page["StackResourceSummaries"]:
            if res["ResourceType"] == "AWS::StepFunctions::StateMachine":
                arn = res["PhysicalResourceId"]
                break
        if arn:
            break
    if not arn:
        raise RuntimeError(f"No state machine found in stack {stack}")
    ctx.board.log(f"Starting DR simulation on {arn.rsplit(':', 1)[-1]}",
                  key=ctx.key)
    execution = sfn.start_execution(
        stateMachineArn=arn,
        input=json.dumps({"simulate_dr": "YES"}))["executionArn"]
    deadline = time.time() + ctx.cfg.dr_simulation_mins * 60
    while time.time() < deadline:
        desc = sfn.describe_execution(executionArn=execution)
        if desc["status"] == "SUCCEEDED":
            ctx.checks["dr_workflow"] = "PASS"
            ctx.timed("dr_simulation", t0)
            return
        if desc["status"] in ("FAILED", "TIMED_OUT", "ABORTED"):
            ctx.checks["dr_workflow"] = f"FAIL ({desc['status']})"
            raise RuntimeError(f"DR workflow ended with {desc['status']}")
        ctx.status(f"DR workflow: {desc['status']}")
        time.sleep(30)
    ctx.checks["dr_workflow"] = "FAIL (timeout)"
    raise RuntimeError("DR workflow did not complete in time")


def verify_restore(ctx: Ctx):
    """Verify the marker variable was restored into the secondary environment."""
    if not getattr(ctx, "marker", None):
        ctx.checks["marker_restored"] = "SKIP (no marker seeded)"
        return
    try:
        out = airflow_get_variable(ctx, ctx.secondary_env,
                                   ctx.cfg.secondary_region, MARKER_VAR)
        if ctx.marker in out:
            ctx.checks["marker_restored"] = "PASS"
        else:
            ctx.checks["marker_restored"] = f"FAIL (got: {out.strip()[:80]})"
    except Exception as e:
        ctx.checks["marker_restored"] = f"FAIL ({e})"


# ============================================================================
# Cleanup
# ============================================================================

def delete_mwaa_env(cfg: Config, board: StatusBoard, env_name: str, region: str):
    mwaa = boto3.client("mwaa", region_name=region)
    # If the env is mid-CREATE, we must wait until it settles before deleting.
    # Give it the full creation budget plus margin.
    deadline = time.time() + (cfg.mwaa_creation_mins + 15) * 60
    polls = 0
    while time.time() < deadline:
        try:
            status = mwaa.get_environment(Name=env_name)["Environment"]["Status"]
        except ClientError:
            return  # already gone
        if status in ("CREATING", "UPDATING", "DELETING"):
            if polls % 5 == 0:  # log every ~5 min, not every poll
                board.log(f"MWAA {env_name} is {status}; waiting to delete...")
            polls += 1
            time.sleep(60)
            continue
        break
    try:
        mwaa.delete_environment(Name=env_name)
        board.log(f"Deleting MWAA env {env_name}...")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            board.log(f"WARN: delete {env_name}: {e}")
        return
    # Wait for deletion (MWAA envs take 20-30 min to delete)
    deadline = time.time() + 40 * 60
    while time.time() < deadline:
        try:
            mwaa.get_environment(Name=env_name)
            time.sleep(60)
        except ClientError:
            board.log(f"MWAA env {env_name} deleted")
            return


def empty_and_delete_bucket(board: StatusBoard, name: str, region: str):
    s3 = boto3.resource("s3", region_name=region)
    try:
        bucket = s3.Bucket(name)
        bucket.object_versions.delete()
        bucket.delete()
        board.log(f"Deleted bucket {name}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("NoSuchBucket", "404"):
            board.log(f"WARN: delete bucket {name}: {e}")


def delete_role(board: StatusBoard, role_name: str):
    iam = boto3.client("iam")
    try:
        for p in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=p)
        for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
        iam.delete_role(RoleName=role_name)
        board.log(f"Deleted role {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            board.log(f"WARN: delete role {role_name}: {e}")


def delete_shared_vpc(cfg: Config, board: StatusBoard, region: str):
    """Delete the shared VPC and all its components."""
    ec2 = boto3.client("ec2", region_name=region)
    vpc_name = f"{cfg.id_prefix}-shared-vpc"
    vpcs = ec2.describe_vpcs(
        Filters=[{"Name": "tag:Name", "Values": [vpc_name]}])["Vpcs"]
    if not vpcs:
        return
    vpc_id = vpcs[0]["VpcId"]
    board.log(f"Deleting shared VPC {vpc_id} in {region}...")
    vpc_filter = [{"Name": "vpc-id", "Values": [vpc_id]}]

    # NAT gateways first (slow to delete)
    nats = ec2.describe_nat_gateways(Filters=vpc_filter)["NatGateways"]
    for nat in nats:
        if nat["State"] not in ("deleted", "deleting"):
            ec2.delete_nat_gateway(NatGatewayId=nat["NatGatewayId"])
    for nat in nats:
        deadline = time.time() + 10 * 60
        while time.time() < deadline:
            state = ec2.describe_nat_gateways(
                NatGatewayIds=[nat["NatGatewayId"]])["NatGateways"][0]["State"]
            if state == "deleted":
                break
            time.sleep(20)

    # Release EIPs tagged for this framework
    for addr in ec2.describe_addresses(Filters=[
            {"Name": f"tag:{TAG_KEY}", "Values": [cfg.id_prefix]}])["Addresses"]:
        try:
            ec2.release_address(AllocationId=addr["AllocationId"])
        except ClientError:
            pass

    # IGWs
    for igw in ec2.describe_internet_gateways(Filters=[
            {"Name": "attachment.vpc-id", "Values": [vpc_id]}])["InternetGateways"]:
        ec2.detach_internet_gateway(
            InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])

    # Route tables (non-main)
    for rtb in ec2.describe_route_tables(Filters=vpc_filter)["RouteTables"]:
        if any(a.get("Main") for a in rtb.get("Associations", [])):
            continue
        for assoc in rtb.get("Associations", []):
            ec2.disassociate_route_table(
                AssociationId=assoc["RouteTableAssociationId"])
        ec2.delete_route_table(RouteTableId=rtb["RouteTableId"])

    # VPC endpoints (created by the DR stack's SFN VPCE option)
    vpces = ec2.describe_vpc_endpoints(Filters=vpc_filter)["VpcEndpoints"]
    if vpces:
        ec2.delete_vpc_endpoints(
            VpcEndpointIds=[v["VpcEndpointId"] for v in vpces])

    # Subnets. Lingering ENIs (Lambda VPC ENIs from destroyed stacks, MWAA
    # leftovers) block deletion for up to ~20 min after their owner is gone.
    # Delete available ENIs ourselves, and retry while AWS releases in-use ones.
    deadline = time.time() + 25 * 60
    pending_subnets = [s["SubnetId"]
                       for s in ec2.describe_subnets(Filters=vpc_filter)["Subnets"]]
    logged_wait = False
    while pending_subnets and time.time() < deadline:
        for subnet_id in list(pending_subnets):
            for eni in ec2.describe_network_interfaces(Filters=[
                    {"Name": "subnet-id", "Values": [subnet_id]}])["NetworkInterfaces"]:
                if eni["Status"] == "available":
                    try:
                        ec2.delete_network_interface(
                            NetworkInterfaceId=eni["NetworkInterfaceId"])
                    except ClientError:
                        pass
            try:
                ec2.delete_subnet(SubnetId=subnet_id)
                pending_subnets.remove(subnet_id)
            except ClientError as e:
                if e.response["Error"]["Code"] != "DependencyViolation":
                    board.log(f"WARN: subnet {subnet_id}: {e}")
                    pending_subnets.remove(subnet_id)
        if pending_subnets:
            if not logged_wait:
                board.log(f"Waiting for lingering ENIs to release in {region} "
                          f"({len(pending_subnets)} subnets blocked, "
                          f"can take ~20 min for Lambda ENIs)...")
                logged_wait = True
            time.sleep(30)
    for subnet_id in pending_subnets:
        board.log(f"WARN: subnet {subnet_id} still blocked by ENIs; "
                  f"rerun --cleanup-only later")

    # Security groups (non-default)
    for sg in ec2.describe_security_groups(Filters=vpc_filter)["SecurityGroups"]:
        if sg["GroupName"] == "default":
            continue
        try:
            ec2.delete_security_group(GroupId=sg["GroupId"])
        except ClientError:
            # revoke self-referencing rules then retry
            try:
                if sg.get("IpPermissions"):
                    ec2.revoke_security_group_ingress(
                        GroupId=sg["GroupId"], IpPermissions=sg["IpPermissions"])
                ec2.delete_security_group(GroupId=sg["GroupId"])
            except ClientError as e2:
                board.log(f"WARN: SG {sg['GroupId']}: {e2}")

    try:
        ec2.delete_vpc(VpcId=vpc_id)
        board.log(f"Deleted shared VPC {vpc_id} in {region}")
    except ClientError as e:
        board.log(f"WARN: VPC {vpc_id} not deleted ({e.response['Error']['Code']}); "
                  f"rerun --cleanup-only after lingering ENIs are released")


def cleanup_version(ctx: Ctx, infra: Infra):
    """Full teardown for one version. Never raises; logs warnings instead."""
    cfg, board = ctx.cfg, ctx.board
    ctx.status("cleanup: CDK stacks")
    try:
        cdk_destroy(ctx, infra)
    except Exception as e:
        board.log(f"WARN: cdk destroy v{ctx.version}: {e}")

    ctx.status("cleanup: MWAA environments")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(delete_mwaa_env, cfg, board, ctx.primary_env, cfg.primary_region)
        f2 = ex.submit(delete_mwaa_env, cfg, board, ctx.secondary_env, cfg.secondary_region)
        f1.result()
        f2.result()

    ctx.status("cleanup: buckets + roles")
    empty_and_delete_bucket(board, ctx.primary_bucket, cfg.primary_region)
    empty_and_delete_bucket(board, ctx.secondary_bucket, cfg.secondary_region)
    delete_role(board, ctx.primary_role)
    delete_role(board, ctx.secondary_role)


def cleanup_everything(cfg: Config, board: StatusBoard):
    """--cleanup-only: discover and delete ALL framework resources by prefix/tag."""
    board.log("Discovering resources to clean up...")

    # CloudFormation stacks FIRST — the stacks' custom resources (e.g. the
    # airflow-cli unpause CR) run against the MWAA envs on Delete; deleting
    # the envs first makes stacks end up DELETE_FAILED and leak resources.
    for region in (cfg.primary_region, cfg.secondary_region):
        cfn = boto3.client("cloudformation", region_name=region)
        to_delete = []
        pages = cfn.get_paginator("list_stacks").paginate(
            StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE",
                               "ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
                               "CREATE_FAILED", "DELETE_FAILED"])
        for page in pages:
            for st in page["StackSummaries"]:
                if st["StackName"].startswith(cfg.id_prefix):
                    board.log(f"Deleting stack {st['StackName']} ({region}, "
                              f"{st['StackStatus']})")
                    if st["StackStatus"] == "DELETE_FAILED":
                        # Retry, retaining the resources that blocked the
                        # previous attempt (typically CRs whose target env
                        # is already gone) so the rest of the stack goes.
                        stuck = [e["LogicalResourceId"] for e in
                                 cfn.describe_stack_events(
                                     StackName=st["StackName"])["StackEvents"]
                                 if e["ResourceStatus"] == "DELETE_FAILED"
                                 and e["LogicalResourceId"] != st["StackName"]]
                        cfn.delete_stack(StackName=st["StackName"],
                                         RetainResources=sorted(set(stuck)))
                    else:
                        cfn.delete_stack(StackName=st["StackName"])
                    to_delete.append(st["StackName"])
        for name in to_delete:
            try:
                cfn.get_waiter("stack_delete_complete").wait(
                    StackName=name,
                    WaiterConfig={"Delay": 30, "MaxAttempts": 60})
                board.log(f"Stack {name} deleted")
            except WaiterError:
                board.log(f"WARN: stack {name} delete did not finish; "
                          f"rerun --cleanup-only")

    # MWAA environments
    ctxs = []
    for region in (cfg.primary_region, cfg.secondary_region):
        mwaa = boto3.client("mwaa", region_name=region)
        for name in mwaa.list_environments()["Environments"]:
            if name.startswith(cfg.id_prefix):
                board.log(f"Will delete MWAA env: {name} ({region})")
                ctxs.append((name, region))
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(delete_mwaa_env, cfg, board, n, r) for n, r in ctxs]
        for f in futures:
            f.result()

    # S3 buckets
    s3 = boto3.client("s3")
    for b in s3.list_buckets()["Buckets"]:
        if b["Name"].startswith(cfg.id_prefix):
            region = s3.get_bucket_location(Bucket=b["Name"]).get(
                "LocationConstraint") or "us-east-1"
            empty_and_delete_bucket(board, b["Name"], region)

    # IAM roles
    iam = boto3.client("iam")
    for page in iam.get_paginator("list_roles").paginate():
        for role in page["Roles"]:
            if role["RoleName"].startswith(cfg.id_prefix):
                delete_role(board, role["RoleName"])

    # Shared VPCs (and any leftover per-version VPCs by name prefix)
    for region in (cfg.primary_region, cfg.secondary_region):
        try:
            delete_shared_vpc(cfg, board, region)
        except Exception as e:
            board.log(f"WARN: shared VPC cleanup ({region}): {e}")
        # legacy per-version VPCs from old runs
        ec2 = boto3.client("ec2", region_name=region)
        for vpc in ec2.describe_vpcs(Filters=[
                {"Name": "tag:Name", "Values": [f"{cfg.id_prefix}-*"]}])["Vpcs"]:
            vname = next((t["Value"] for t in vpc.get("Tags", [])
                          if t["Key"] == "Name"), "")
            if vname != f"{cfg.id_prefix}-shared-vpc":
                board.log(f"Found legacy VPC {vpc['VpcId']} ({vname}), deleting...")
                _force_delete_vpc(board, region, vpc["VpcId"])

    board.log("Cleanup complete.")


def _force_delete_vpc(board: StatusBoard, region: str, vpc_id: str):
    """Best-effort deletion of an arbitrary VPC and its dependencies."""
    ec2 = boto3.client("ec2", region_name=region)
    f = [{"Name": "vpc-id", "Values": [vpc_id]}]
    try:
        # Delete NATs and WAIT for them (IGW detach fails while NAT holds an EIP)
        nats = ec2.describe_nat_gateways(Filters=f)["NatGateways"]
        for nat in nats:
            if nat["State"] not in ("deleted", "deleting"):
                ec2.delete_nat_gateway(NatGatewayId=nat["NatGatewayId"])
        for nat in nats:
            deadline = time.time() + 10 * 60
            while time.time() < deadline:
                state = ec2.describe_nat_gateways(
                    NatGatewayIds=[nat["NatGatewayId"]])["NatGateways"][0]["State"]
                if state == "deleted":
                    break
                time.sleep(15)
        # Release any EIPs that were attached to those NATs
        for nat in nats:
            for addr in nat.get("NatGatewayAddresses", []):
                alloc = addr.get("AllocationId")
                if alloc:
                    try:
                        ec2.release_address(AllocationId=alloc)
                    except ClientError:
                        pass
        for igw in ec2.describe_internet_gateways(Filters=[
                {"Name": "attachment.vpc-id", "Values": [vpc_id]}])["InternetGateways"]:
            ec2.detach_internet_gateway(
                InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])
        for rtb in ec2.describe_route_tables(Filters=f)["RouteTables"]:
            if any(a.get("Main") for a in rtb.get("Associations", [])):
                continue
            for assoc in rtb.get("Associations", []):
                ec2.disassociate_route_table(
                    AssociationId=assoc["RouteTableAssociationId"])
            ec2.delete_route_table(RouteTableId=rtb["RouteTableId"])
        for sub in ec2.describe_subnets(Filters=f)["Subnets"]:
            ec2.delete_subnet(SubnetId=sub["SubnetId"])
        for sg in ec2.describe_security_groups(Filters=f)["SecurityGroups"]:
            if sg["GroupName"] != "default":
                try:
                    ec2.delete_security_group(GroupId=sg["GroupId"])
                except ClientError:
                    pass
        ec2.delete_vpc(VpcId=vpc_id)
        board.log(f"Deleted VPC {vpc_id}")
    except ClientError as e:
        board.log(f"WARN: could not fully delete VPC {vpc_id}: {e} "
                  f"(re-run --cleanup-only later)")


# ============================================================================
# Reporting
# ============================================================================

def write_report(cfg: Config, results: list, log_dir: Path) -> Path:
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "account_id": cfg.account_id,
        "primary_region": cfg.primary_region,
        "secondary_region": cfg.secondary_region,
        "results": results,
    }
    path = log_dir / "test_report.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    return path


def print_table(results: list):
    print("\n" + "=" * 78)
    print(f"{'VERSION':<10} {'STRATEGY':<14} {'RESULT':<8} {'DURATION':<10} CHECKS")
    print("-" * 78)
    for r in results:
        checks = ", ".join(f"{k}={v}" for k, v in r["checks"].items()) or "-"
        mins = r.get("duration_secs", 0) / 60
        print(f"{r['version']:<10} {r['strategy']:<14} {r['result']:<8} "
              f"{mins:>6.1f}m   {checks}")
        for err in r.get("errors", []):
            print(f"{'':<10} ERROR: {err[:100]}")
    print("=" * 78 + "\n")


def bedrock_summary(cfg: Config, results: list, board: StatusBoard):
    if not cfg.bedrock_enabled:
        return
    try:
        rt = boto3.client("bedrock-runtime", region_name=cfg.bedrock_region)
        prompt = (
            "You are analyzing E2E test results for an MWAA disaster recovery "
            "solution. Summarize: overall health, per-version outcomes, any "
            "bugs/gaps the failures suggest in the DR solution itself, and "
            "recommended next steps. Be concise.\n\nResults JSON:\n"
            + json.dumps(results, indent=2, default=str))
        resp = rt.invoke_model(
            modelId=cfg.bedrock_model_id,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                "messages": [{"role": "user", "content": prompt}],
            }))
        text = json.loads(resp["body"].read())["content"][0]["text"]
        print("\n───────────  AI SUMMARY (Bedrock)  ───────────")
        print(text)
        print("───────────────────────────────────────────────\n")
        (board.log_dir / "bedrock_summary.txt").write_text(text)
    except Exception as e:
        board.log(f"Bedrock summary skipped: {e}")


# ============================================================================
# Per-version test pipeline
# ============================================================================

def run_version(ctx: Ctx, infra: Infra, teardown: bool = False,
                provision: bool = True) -> dict:
    t0 = time.time()
    result = "PASS"
    reused = False
    try:
        if not provision and infra_ready(ctx):
            ctx.board.log("Existing MWAA envs AVAILABLE — reusing infrastructure "
                          "(redeploying DR solution only)", key=ctx.key)
            adopt_existing_infra(ctx)
            reused = True
        else:
            if not provision:
                ctx.board.log("No reusable MWAA envs found — provisioning",
                              key=ctx.key)
            infra.provision_for_version(ctx)
            deploy_mwaa(ctx, infra)
        cdk_deploy(ctx, infra)
        seed_test_data(ctx)
        trigger_and_wait_backup(ctx)
        wait_replication(ctx)
        simulate_dr(ctx)
        verify_restore(ctx)
        if any(str(v).startswith("FAIL") for v in ctx.checks.values()):
            result = "FAIL"
    except Exception as e:
        result = "FAIL"
        ctx.errors.append(str(e))
        ctx.board.log(f"FAILED: {e}", key=ctx.key)
    finally:
        cleaned = False
        if teardown and result == "PASS":
            try:
                cleanup_version(ctx, infra)
                cleaned = True
            except Exception as e:
                ctx.board.log(f"WARN cleanup: {e}", key=ctx.key)
        elif teardown:
            ctx.board.log("FAIL — resources KEPT despite --teardown so you "
                          "can fix and rerun; use --cleanup-only to remove.",
                          key=ctx.key)
        else:
            # Default: keep everything. The framework exists to iterate on
            # the DR solution — reruns adopt these envs and retest in
            # minutes instead of re-provisioning for ~1h.
            ctx.board.log(f"{result} — infrastructure kept. Rerun "
                          f"./run_e2e.py to test again on the same envs; "
                          f"--cleanup-only removes everything.", key=ctx.key)
    ctx.board.finish(ctx.key, result)
    return {
        "version": ctx.version,
        "strategy": ctx.strategy,
        "result": result,
        "infra_reused": reused,
        "cleaned": cleaned,
        "duration_secs": round(time.time() - t0, 1),
        "checks": ctx.checks,
        "errors": ctx.errors,
        "timings": ctx.timings,
    }


# ============================================================================
# Main
# ============================================================================

def dry_run_plan(cfg: Config):
    print("\n─────────  DRY RUN — execution plan  ─────────")
    print(f"Account: {cfg.account_id} | {cfg.primary_region} → {cfg.secondary_region}")
    print(f"Mode: {'PARALLEL' if cfg.parallel else 'SEQUENTIAL'}")
    print("\n1. CDK bootstrap both regions")
    print("2. Create 1 shared VPC per region (2 total: IGW+NAT+2 private subnets)")
    for v in cfg.versions:
        for s in cfg.dr_strategies:
            p = f"{cfg.id_prefix}-{slug(v)}"
            print(f"\n── v{v} / {s} ──")
            print(f"3. Buckets: {cfg.id_prefix}-{cfg.account_id[:6]}-{slug(v)}-{{pri,sec}}-dags")
            print(f"4. Roles:   {p}-{{pri,sec}}-role")
            print(f"5. MWAA:    {p}-primary ({cfg.primary_region}), "
                  f"{p}-secondary ({cfg.secondary_region})  [~30 min]")
            print(f"6. CDK:     {p}-primary-stack + {p}-secondary-stack")
            print("7. Seed marker variable → trigger backup → wait replication")
            print("8. Start recovery SFN with simulate_dr=YES → wait success")
            print("9. Verify marker variable restored in secondary env")
            print("10. Keep infra for reruns (pass --teardown to destroy "
                  "stacks, MWAA envs, buckets, roles on PASS)")
    print("\nFinal: delete shared VPCs, write JSON report, print table, Bedrock summary")
    print("──────────────────────────────────────────────\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Show plan, do nothing")
    ap.add_argument("--cleanup-only", action="store_true",
                    help="Delete all e2e resources and exit")
    ap.add_argument("--teardown", action="store_true",
                    help="Tear down a version's resources after it PASSES "
                         "(and shared VPCs when everything passed). Default "
                         "is to KEEP all infrastructure so the script can be "
                         "rerun repeatedly against the same MWAA envs; use "
                         "--cleanup-only to remove everything.")
    ap.add_argument("--provision-infrastructure", action="store_true",
                    help="Force full infra provisioning (VPCs, buckets, roles, "
                         "MWAA envs) even if they already exist. Without this "
                         "flag, existing AVAILABLE MWAA envs are reused: only "
                         "the DR solution is redeployed and retested, and the "
                         "infra is kept afterwards.")
    ap.add_argument("--versions", nargs="+", help="Override versions to test")
    ap.add_argument("--regions", nargs=2, metavar=("PRIMARY", "SECONDARY"),
                    help="Override primary/secondary regions from the config. "
                         "Handy with --cleanup-only to remove leftovers in "
                         "previously used regions after a config change.")
    ap.add_argument("--sequential", action="store_true", help="Force sequential mode")
    ap.add_argument("--config", default=str(SCRIPT_DIR / "e2e_config.yaml"))
    args = ap.parse_args()

    cfg = Config.load(Path(args.config))
    if args.versions:
        cfg.versions = args.versions
    if args.regions:
        cfg.primary_region, cfg.secondary_region = args.regions
    if args.sequential:
        cfg.parallel = False

    if args.dry_run:
        dry_run_plan(cfg)
        return 0

    log_dir = SCRIPT_DIR / "logs" / datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    board = StatusBoard(log_dir)
    board.log(f"Run started | account={cfg.account_id} | "
              f"versions={cfg.versions} | parallel={cfg.parallel} | logs={log_dir}")

    if args.cleanup_only:
        try:
            cleanup_everything(cfg, board)
            return 0
        except KeyboardInterrupt:
            print("\n[ABORT] Cleanup interrupted — rerun --cleanup-only to finish.")
            return 130
        except Exception as e:
            board.logfile.write(traceback.format_exc())
            board.logfile.flush()
            board.log(f"FATAL during cleanup: {e}")
            board.log("Rerun './run_e2e.py --cleanup-only' to remove what's left "
                      f"(full traceback in {board.logfile.name}).")
            return 1

    try:
        # Preflight: the CDK app needs aws_cdk importable by the interpreter
        # we pin via --app (this script's interpreter)
        rc = subprocess.run([sys.executable, "-c", "import aws_cdk"],
                            capture_output=True).returncode
        if rc != 0:
            board.log(f"ERROR: `{sys.executable}` cannot import aws_cdk — "
                      "the CDK app (app.py) will fail to synth.")
            board.log(f"Fix: {sys.executable} -m pip install -r requirements.txt   (from repo root)")
            return 1
        if subprocess.run(["npx", "cdk", "--version"],
                          capture_output=True).returncode != 0:
            board.log("ERROR: `npx cdk` not available. Install Node.js + CDK.")
            return 1

        # CDK bootstrap
        board.log("CDK bootstrap...")
        rc = run_cmd(["npx", "cdk", "bootstrap",
                      "--app", CDK_APP,
                      f"aws://{cfg.account_id}/{cfg.primary_region}",
                      f"aws://{cfg.account_id}/{cfg.secondary_region}"],
                     cwd=REPO_ROOT, log_path=log_dir / "bootstrap.log",
                     timeout_secs=600)
        if rc != 0:
            board.log("ERROR: CDK bootstrap failed, see bootstrap.log")
            return 1

        # Shared VPCs (concurrent across the 2 regions)
        infra = Infra(cfg, board)
        board.log("Provisioning shared VPCs (NAT gateways take ~2 min)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(infra.ensure_shared_vpc, cfg.primary_region, 192)
            f2 = ex.submit(infra.ensure_shared_vpc, cfg.secondary_region, 193)
            f1.result()
            f2.result()

        # Version tests
        board.start_printer()
        ctxs = [Ctx(version=v, strategy=s, cfg=cfg, board=board, log_dir=log_dir)
                for v in cfg.versions for s in cfg.dr_strategies]
        results = []
        if cfg.parallel and len(ctxs) > 1:
            board.log(f"Running {len(ctxs)} tests in PARALLEL")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(ctxs)) as ex:
                futures = {ex.submit(run_version, c, infra, args.teardown,
                                     args.provision_infrastructure): c
                           for c in ctxs}
                for fut in concurrent.futures.as_completed(futures):
                    results.append(fut.result())
        else:
            board.log(f"Running {len(ctxs)} tests SEQUENTIALLY")
            for c in ctxs:
                results.append(run_version(c, infra, args.teardown,
                                           args.provision_infrastructure))

        board.stop_printer()

        # Shared VPC cleanup — only with --teardown and when every version
        # actually tore its resources down; anything kept still needs them.
        if args.teardown and results and all(r.get("cleaned") for r in results):
            board.log("Deleting shared VPCs...")
            for region in (cfg.primary_region, cfg.secondary_region):
                try:
                    delete_shared_vpc(cfg, board, region)
                except Exception as e:
                    board.log(f"WARN: shared VPC cleanup ({region}): {e}")
        else:
            board.log("Infrastructure kept. Rerun './run_e2e.py' to test "
                      "again on the same envs, or "
                      "'./run_e2e.py --cleanup-only' to remove everything.")

        # Reporting
        results.sort(key=lambda r: r["version"])
        report_path = write_report(cfg, results, log_dir)
        print_table(results)
        bedrock_summary(cfg, results, board)
        board.log(f"Report: {report_path}")

        return 0 if all(r["result"] == "PASS" for r in results) else 1

    except KeyboardInterrupt:
        print("\n[ABORT] Interrupted — terminating child processes...")
        kill_children()
        print("[ABORT] Resources may be left running. "
              "Run './run_e2e.py --cleanup-only' to remove them.")
        return 130
    except Exception as e:
        # Graceful fatal: concise message on the console, full traceback
        # only in the log file.
        board.stop_printer()
        board.logfile.write(traceback.format_exc())
        board.logfile.flush()
        board.log(f"FATAL: {e}")
        board.log(f"Full traceback in {board.logfile.name}. Resources already "
                  f"created are kept — './run_e2e.py --cleanup-only' removes them.")
        kill_children()
        return 1


if __name__ == "__main__":
    sys.exit(main())

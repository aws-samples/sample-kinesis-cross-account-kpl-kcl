# Threat Model: Kinesis Cross-Account KPL/KCL Sample

## 1. Business Context

**What it does:** Sample code demonstrating cross-account streaming with Amazon Kinesis Data Streams. A KPL producer in Account A writes stock trade records to a stream in Account B using STS AssumeRole. A KCL consumer in Account C reads from the same stream using a resource-based policy.

**Deployment environment:** Three separate AWS accounts, single region, EC2 instances in default VPCs.

**Data sensitivity:** Sample/synthetic stock trade data (ticker, price, quantity, timestamp). No PII, no customer data. However, customers adopting this pattern may process sensitive data.

**Integration complexity:** Medium — crosses two account boundaries, involves KMS encryption, IAM roles, resource policies, and DynamoDB metadata tables.

**Intended audience:** AWS customers learning cross-account streaming patterns. Published as open-source sample code.

---

## 2. Architecture Components

| ID | Component | Account | Type |
|---|---|---|---|
| C1 | KPL Producer Application | A (Producer) | EC2 Instance (t3.medium) |
| C2 | KPLProducerRole | A (Producer) | IAM Role (Instance Profile) |
| C3 | KinesisProducerCrossAccountRole | B (Stream) | IAM Role (Cross-Account) |
| C4 | Kinesis Data Stream (StockTradeStream) | B (Stream) | Managed Service |
| C5 | Customer-Managed KMS Key | B (Stream) | KMS Key |
| C6 | Stream Resource Policy | B (Stream) | Resource Policy |
| C7 | EFO Consumer (pre-registered) | B (Stream) | Stream Consumer |
| C8 | EFO Consumer Resource Policy | B (Stream) | Resource Policy |
| C9 | KCL Consumer Application | C (Consumer) | EC2 Instance (t3.medium) |
| C10 | KCLConsumerRole | C (Consumer) | IAM Role (Instance Profile) |
| C11 | DynamoDB Lease Tables | C (Consumer) | Managed Service |
| C12 | CloudWatch Metrics | A & C | Managed Service |

---

## 3. Trust Zones

| Zone | Components | Description |
|---|---|---|
| TZ1: Producer Account (A) | C1, C2, C12 | EC2 instance with KPL app; trusts local IAM |
| TZ2: Stream Account (B) | C3, C4, C5, C6, C7, C8 | Owns stream and access control; trust anchor |
| TZ3: Consumer Account (C) | C9, C10, C11, C12 | EC2 instance with KCL app; trusts local IAM |

---

## 4. Data Flows & Crossing Points

| Flow | From → To | Crosses | Auth Method |
|---|---|---|---|
| F1 | C1 → C3 | TZ1 → TZ2 | STS AssumeRole with external ID |
| F2 | C1 (via C3) → C4 | Within TZ2 | IAM policy on C3 |
| F3 | C4 → C9 | TZ2 → TZ3 | Resource-based policy (C6) + identity policy (C10) |
| F4 | C1 → C5 | TZ1 → TZ2 | KMS key policy (GenerateDataKey) |
| F5 | C4 → C9 (decrypt) | TZ2 → TZ3 | KMS key policy (Decrypt) |
| F6 | C9 → C11 | Within TZ3 | IAM policy on C10 |
| F7 | C1/C9 → C12 | Within TZ1/TZ3 | IAM policy (PutMetricData) |

---

## 5. Assumptions

| ID | Assumption |
|---|---|
| A1 | All communication between EC2 instances and AWS services uses TLS (enforced by AWS SDKs) |
| A2 | EC2 instances are in private subnets or default VPC with security groups restricting inbound access |
| A3 | IMDSv2 is enforced (HttpTokens: required) preventing SSRF credential theft |
| A4 | KMS key rotation is enabled (automatic annual rotation) |
| A5 | The external ID is a non-guessable shared secret between Account A and Account B, supplied at deploy time (`ExternalId` parameter / `KINESIS_EXTERNAL_ID`) and never hardcoded; use a unique per-deployment value |
| A6 | IAM users deploying stacks have appropriate permissions; the sample does not manage deployer IAM |
| A7 | EC2 instances are ephemeral (for demo); production deployments should use Auto Scaling Groups |

---

## 6. Threats (STRIDE)

### Spoofing

| ID | Threat | Severity | Component |
|---|---|---|---|
| T1 | **Confused deputy attack on cross-account role** — A malicious service tricks Account B into granting access to an unauthorized principal | HIGH | C3 |
| T2 | **EC2 instance metadata theft (SSRF)** — Attacker exploits application vulnerability to steal instance profile credentials from IMDS | MEDIUM | C1, C9 |

### Tampering

| ID | Threat | Severity | Component |
|---|---|---|---|
| T3 | **Stream data tampering via over-privileged producer role** — If the producer role has broader permissions than needed, a compromised instance could modify stream configuration | MEDIUM | C3 |
| T4 | **DynamoDB lease table poisoning** — Attacker with write access to lease tables could manipulate shard assignments or checkpoints | MEDIUM | C11 |

### Repudiation

| ID | Threat | Severity | Component |
|---|---|---|---|
| T5 | **Unattributed cross-account writes** — Without CloudTrail, writes from Account A via assumed role cannot be traced to the originating principal | LOW | C3, C4 |

### Information Disclosure

| ID | Threat | Severity | Component |
|---|---|---|---|
| T6 | **KMS key policy overly broad** — Key policy grants to account root; any principal in the account can potentially use the key | MEDIUM | C5 |
| T7 | **Stream data exposure via overly broad resource policy** — Resource policy grants to account root rather than specific IAM role | MEDIUM | C6 |
| T8 | **Credentials in git history** — Internal GitLab URLs in commit history could leak internal infrastructure details if repo history is published as-is | LOW | N/A |

### Denial of Service

| ID | Threat | Severity | Component |
|---|---|---|---|
| T9 | **Shard exhaustion by rogue producer** — Compromised producer writes at maximum rate, exhausting shard capacity and blocking legitimate traffic | MEDIUM | C4 |
| T10 | **DynamoDB throttling on lease tables** — KCL metadata operations could be throttled under high lease-churn scenarios | LOW | C11 |

### Elevation of Privilege

| ID | Threat | Severity | Component |
|---|---|---|---|
| T11 | **Producer role escalation** — KPLProducerRole has `sts:AssumeRole` on the cross-account role; if instance is compromised, attacker gains write access to the stream | MEDIUM | C2, C3 |
| T12 | **Wildcard resource on CloudWatch/STS actions** — `cloudwatch:PutMetricData` and `sts:GetCallerIdentity` use `Resource: '*'` which cannot be scoped further | LOW | C2, C10 |

---

## 7. Mitigations

| ID | Mitigates | Control Type | Description |
|---|---|---|---|
| M1 | T1 | Preventive | **External ID condition** on the AssumeRole trust policy (`sts:ExternalId`, supplied via the `ExternalId` deploy parameter — non-guessable, never hardcoded) prevents confused deputy attacks |
| M2 | T2 | Preventive | **IMDSv2 enforced** via `HttpTokens: required` in CloudFormation; blocks SSRF-based credential theft from IMDSv1 |
| M3 | T3 | Preventive | **Least-privilege producer role** — only grants `PutRecord`, `PutRecords`, `DescribeStream`, `DescribeStreamSummary`, `ListShards` on the specific stream ARN; no admin or config-change actions |
| M4 | T4 | Preventive | **DynamoDB resource-scoped IAM** — consumer role can only access specific named tables; no wildcard table access |
| M5 | T5 | Detective | **CloudTrail logging** — AWS CloudTrail is enabled by default in all accounts; AssumeRole events and Kinesis API calls are logged with source identity |
| M6 | T6 | Preventive | **Scoped KMS key grants** — key policy grants only `GenerateDataKey*` and `Decrypt` to producer account, only `Decrypt` to consumer account; not `kms:*` |
| M7 | T7 | Accepted (sample limitation) | **Resource policy grants to account root** — this is standard for cross-account resource policies. In production, scope to specific IAM roles instead of account root. Documented as a best-practice note. |
| M8 | T8 | Preventive | **Squash history before public push** — documented requirement to rewrite git history when pushing to aws-samples GitHub (removes internal URLs) |
| M9 | T9 | Preventive | **KPL rate limiting** — `rateLimit(150)` caps producer throughput; Kinesis service quotas provide further protection. Stream has provisioned capacity (2 shards = 2 MB/s) |
| M10 | T10 | Preventive | **DynamoDB on-demand mode** — lease tables use PAY_PER_REQUEST billing, which auto-scales to handle burst lease operations |
| M11 | T11 | Preventive | **Scoped AssumeRole** — KPLProducerRole can only assume the one specific cross-account role ARN (not `*`); the assumed role itself is scoped to stream-only actions |
| M12 | T12 | Accepted (AWS limitation) | **Cannot scope further** — `cloudwatch:PutMetricData` and `sts:GetCallerIdentity` do not support resource-level permissions per AWS documentation. Suppressed with cfn_nag justification. |

---

## 8. Residual Risks

| Risk | Severity | Acceptance Rationale |
|---|---|---|
| Resource policy grants to account root (T7) | MEDIUM | Standard AWS pattern for cross-account resource policies; documented as production hardening recommendation |
| Wildcard resource on CW/STS (T12) | LOW | AWS service limitation; no alternative exists |
| Sample external ID is static/public (part of A5) | LOW | Documented that production deployments should use unique per-customer external IDs |

---

## 9. Recommendations for Production Adoption

1. **Scope resource policies to specific IAM roles** rather than account root
2. **Use unique external IDs per customer/tenant** rather than the static sample value
3. **Enable VPC endpoints** for Kinesis, DynamoDB, KMS, and STS to keep traffic off the public internet
4. **Add VPC security groups** restricting EC2 outbound to only required AWS service endpoints
5. **Enable GuardDuty** in all three accounts for anomaly detection
6. **Set CloudWatch alarms** on `WriteProvisionedThroughputExceeded` and `ReadProvisionedThroughputExceeded`
7. **Consider AWS PrivateLink** for cross-account stream access in sensitive environments
8. **Rotate KMS key** — already configured (annual auto-rotation enabled)

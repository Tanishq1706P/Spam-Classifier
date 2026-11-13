"""Central error catalogue. Format: SPM-<MODULE>-<NNN>. Documented in docs/errors.md."""
from enum import Enum


class ErrorCode(str, Enum):
    CLU_101 = "SPM-CLU-101"; CLU_102 = "SPM-CLU-102"; CLU_103 = "SPM-CLU-103"
    CLU_104 = "SPM-CLU-104"; CLU_105 = "SPM-CLU-105"
    CFG_001 = "SPM-CFG-001"; CFG_002 = "SPM-CFG-002"
    ING_201 = "SPM-ING-201"; ING_202 = "SPM-ING-202"; ING_203 = "SPM-ING-203"; ING_204 = "SPM-ING-204"
    PRE_301 = "SPM-PRE-301"; PRE_302 = "SPM-PRE-302"
    FEA_401 = "SPM-FEA-401"; FEA_402 = "SPM-FEA-402"
    TRN_501 = "SPM-TRN-501"; TRN_502 = "SPM-TRN-502"; TRN_503 = "SPM-TRN-503"
    EVL_601 = "SPM-EVL-601"; EVL_602 = "SPM-EVL-602"
    MOD_701 = "SPM-MOD-701"; MOD_702 = "SPM-MOD-702"
    STR_801 = "SPM-STR-801"; STR_802 = "SPM-STR-802"; STR_803 = "SPM-STR-803"
    API_901 = "SPM-API-901"; API_902 = "SPM-API-902"; API_903 = "SPM-API-903"


# code -> (description, hint)
CATALOGUE = {
    "SPM-CLU-101": ("HDFS NameNode down", "Run start-dfs.sh on master; check jps"),
    "SPM-CLU-102": ("DataNode missing", "Check worker logs; verify workers file"),
    "SPM-CLU-103": ("YARN unreachable", "Run start-yarn.sh; check ResourceManager on master:8088"),
    "SPM-CLU-104": ("Kafka broker down", "Start kafka-server-start.sh with KRaft config"),
    "SPM-CLU-105": ("HBase master down", "Run start-hbase.sh; check ZooKeeper"),
    "SPM-CFG-001": ("Config file missing", "Check config/settings.yaml or SPAM_CONFIG"),
    "SPM-CFG-002": ("Invalid config key", "Compare key with config/settings.yaml"),
    "SPM-ING-201": ("Input file not found", "Check path and permissions"),
    "SPM-ING-202": ("Bad dataset schema", "CSV needs columns: label,text"),
    "SPM-ING-203": ("HDFS write failed", "Check hdfs dfsadmin -report and free space"),
    "SPM-ING-204": ("Kafka send failed", "Verify broker address and topic"),
    "SPM-PRE-301": ("Dataset empty after cleaning", "Inspect raw data for empty/null text"),
    "SPM-PRE-302": ("Encoding error", "Re-save file as UTF-8"),
    "SPM-FEA-401": ("Tokenizer failed", "Check text column type"),
    "SPM-FEA-402": ("Empty vocabulary", "Dataset may contain only stop words"),
    "SPM-TRN-501": ("Executor out of memory", "Increase spark.executor.memory or lower numFeatures"),
    "SPM-TRN-502": ("Severe class imbalance", "Enable balance_classes or add data"),
    "SPM-TRN-503": ("Training did not converge", "Increase maxIter or regParam"),
    "SPM-EVL-601": ("Metric below threshold", "Tune hyperparameters or add data"),
    "SPM-EVL-602": ("Test set empty", "Check train/test split ratio"),
    "SPM-MOD-701": ("Model save failed", "Check HDFS /models permissions"),
    "SPM-MOD-702": ("Model not found", "Train a model first (POST /api/v1/train)"),
    "SPM-STR-801": ("Kafka offset error", "Reset checkpoint dir or offsets"),
    "SPM-STR-802": ("Model load failed", "Verify model path in registry"),
    "SPM-STR-803": ("HBase write failed", "Check HBase Thrift server on port 9090"),
    "SPM-API-901": ("Request validation failed", "Check JSON body and text length"),
    "SPM-API-902": ("Job conflict", "Wait for the running job to finish"),
    "SPM-API-903": ("Operation timed out", "Retry; check cluster load"),
}


def describe(code):
    code = code.value if isinstance(code, ErrorCode) else code
    return CATALOGUE.get(code, ("Unknown error", "See docs/errors.md"))


class SpamError(Exception):
    """The only exception type raised by project code."""

    def __init__(self, code, message=None, cause=""):
        self.code = code.value if isinstance(code, ErrorCode) else code
        desc, hint = describe(self.code)
        self.message = message or desc
        self.cause = str(cause)
        self.hint = hint
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self, phase=""):
        return {"error_code": self.code, "phase": phase, "message": self.message,
                "cause": self.cause, "hint": self.hint}

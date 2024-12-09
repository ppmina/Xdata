from crypto_data.models import enums
from datetime import datetime, timedelta
from crypto_data.utils import gen_sample_time

univ_mapping = {
    "ZZQZ": "000985.SH",
    "ZZ500": "000905.SH",
    "ZZ300": "000300.SH",
    "ZZ1000": "000852.SH",
    "CL60": "CRYPTO.liqmkt60",
    "CALL": "CRYPTO.all",
}

freq_name_mapping = {
    "D1": "1d",
    "H4": "4h",
    "H1": "1h",
    "M30": "30m",
    "M15": "15m",
    "M5": "5m",
    "M3": "3m",
    "M1": "1m",
    "S3": "3s",
    "S1": "1s",
}

freq_mapping_CN = {
    enums.Freq.S1: gen_sample_time(enums.Freq.S1.name, enums.Market.CN),
    enums.Freq.S3: gen_sample_time(enums.Freq.S3.name, enums.Market.CN),
    enums.Freq.M30: gen_sample_time(enums.Freq.M30.name, enums.Market.CN),
    enums.Freq.D1: ["15:00:00.000000"],
}

freq_mapping_CRYPTO = {
    enums.Freq.S1: gen_sample_time(enums.Freq.S1.name, enums.Market.CRYPTO),
    enums.Freq.S3: gen_sample_time(enums.Freq.S3.name, enums.Market.CRYPTO),
    enums.Freq.M5: gen_sample_time(enums.Freq.M5.name, enums.Market.CRYPTO),
    enums.Freq.M15: gen_sample_time(enums.Freq.M15.name, enums.Market.CRYPTO),
    enums.Freq.M30: gen_sample_time(enums.Freq.M30.name, enums.Market.CRYPTO),
    enums.Freq.H1: gen_sample_time(enums.Freq.H1.name, enums.Market.CRYPTO),
    enums.Freq.H4: gen_sample_time(enums.Freq.H4.name, enums.Market.CRYPTO),
    enums.Freq.D1: ["24:00:00.000000"],
}


freq_mapping = {"CRYPTO": freq_mapping_CRYPTO, "CN": freq_mapping_CN}

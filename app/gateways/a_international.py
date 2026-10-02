from .a_domestic import DomesticAdapter


class InternationalAdapter(DomesticAdapter):
    realm = "intl"
    source_evidence = "wb_identity.py:52-62,76-105; wb_accounts.py:119-131"

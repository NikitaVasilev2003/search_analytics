class DomainsAnalyser:
    def __init__(self, domains_order: dict[str, list[int]]) -> None:
        self.domains_order = domains_order

    def domain_positions(self, domain: str) -> list[int]:
        return self.domains_order.get(domain, [])

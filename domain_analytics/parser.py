import re


DOMAIN_FORMAT = r'<domain>(.*?)</domain>'


class Parser:
    def __init__(self, format: str = DOMAIN_FORMAT):
        self.format = format
    
    def parse(self, text: str) -> list[str]:
        return re.findall(self.format, text)

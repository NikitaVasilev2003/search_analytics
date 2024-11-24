import logging
import typing as tp
from dataclasses import dataclass, field

import requests
import tenacity

from domain_analytics.parser import Parser


BASE_URL = 'https://yandex.com/search/xml/'

logger = logging.getLogger(__name__)

@dataclass
class QueryParams:
    query: str
    sortby: str = 'rlv'
    region_identifier: int = 225
    mode: tp.Literal['deep', 'flat'] = 'deep'
    grops_on_page: int | None = None
    docs_in_group: int | None = None
    maxpassages: int = 1

    def __post_init__(self) -> None:
        if self.mode == 'flat':
            self.groupby = f'attr=d.mode=flat'
            return

        grops_on_page = self.grops_on_page or 10
        docs_in_group = self.docs_in_group or 1
        self.groupby = f'attr=d.mode={self.mode}.groups-on-page={grops_on_page}.docs-in-group={docs_in_group}'


@dataclass
class Domains:
    domains_frequency: dict[str, int] = field(default_factory=dict)

    def add(self, domain: str) -> None:
        self.domains_frequency[domain] = self.domains_frequency.get(domain, 0) + 1

    def __len__(self) -> int:
        return len(self.domains_frequency)


class SearchApiClient:
    def __init__(
        self,
        folder_id: str,
        api_token: str,
        timeout: tuple[float, float] | None = None,  # (connect, read) timeout
    ) -> None:
        self.folder_id = folder_id
        self.api_key = api_token
        self.session = requests.Session()
        self.timeout = timeout or (5, 15)
        self.parser = Parser()

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(requests.RequestException),
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(wait=1),
    )
    def _get(
        self,
        params: dict[str, tp.Any] | None = None,
    ) -> requests.Response:
        logger.debug(f'Run request with params: {params}')

        response = self.session.get(
            url=BASE_URL,
            params=params or {},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response

    def _domains_on_page(
        self,
        query_params: QueryParams,
        page: int = 0,
    ) -> list[str]:
        response = self._get(
            params={
                'folderid': self.folder_id,
                'apikey': self.api_key,
                'query': query_params.query,
                'sortby': query_params.sortby,
                'lr': query_params.region_identifier,
                'groupby': query_params.groupby,
                'maxpassages': query_params.maxpassages,
                'page': page,
            },
        )
        return self.parser.parse(response.text)

    def domains(
        self,
        query_params: QueryParams,
        count: int,
    ) -> dict[str, list[int]]:
        assert count > 0, 'Count must be greater than 0'

        domains_order: dict[str, list[int]] = {}
        domains_count = 0
        page = 0

        while True:
            domains_on_page = self._domains_on_page(query_params, page=page)
            assert domains_on_page, f'No domains on page {page}'

            for domain in domains_on_page:
                domains_order.setdefault(domain, []).append(domains_count)
                domains_count += 1
                if domains_count == count:
                    return domains_order
            page += 1

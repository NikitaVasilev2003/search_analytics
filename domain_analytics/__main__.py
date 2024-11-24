import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click

from domain_analytics.analyser import DomainsAnalyser
from domain_analytics.client import QueryParams, SearchApiClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

THREADPOOL_SIZE = 10


def read_txt(file_path: Path) -> list[str]:
    with open(file_path, 'r') as f:
        return f.read().splitlines()


def write_csv(file_path: str, rows: list[list[str]]) -> None:
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def analyse_domains(
    client: SearchApiClient,
    query: str,
    domains: list[str],
    row_index: int,
    region_identifier: int,
    top_domains: int,
) -> tuple[int, list[str]]:
    logger.info(f'Analyzing query "{query}"')

    domains_analyser = DomainsAnalyser(
        domains_order=client.domains(
            query_params=QueryParams(
                query=query,
                region_identifier=region_identifier,
            ),
            count=top_domains,
        )
    )
    logger.debug(f'Domains order: "{domains_analyser.domains_order}"')

    row_content = [query]
    for domain in domains:
        if domain_positions := domains_analyser.domain_positions(domain):
            row_content.append(', '.join(str(position + 1) for position in domain_positions))
        else:
            row_content.append('-')

    return row_index, row_content


@click.command()
@click.option('--api-key', '-k', help='API key')
@click.option('--folder-id', '-f', help='Folder ID')
@click.option('--domains', '-d', help='Domains file path')
@click.option('--queries', '-q', help='Queries file path')
@click.option('--output', '-o', help='Output file path')
@click.option(
    '--region',
    '-r',
    help='Region Identifier https://yandex.cloud/ru/docs/search-api/reference/regions',
    default=225,
    type=int,
)
@click.option(
    '--top-domains',
    '-t',
    help='Top domains count',
    default=10,
    type=int,
)
def main(
    api_key: str,
    folder_id: str,
    domains: str,
    queries: str,
    output: str,
    region: int,
    top_domains: int,
):
    client = SearchApiClient(api_key, folder_id)

    domains = read_txt(domains)
    queries = read_txt(queries)

    results = [None] * (len(queries) + 1)
    results[0] = ['queries'] + domains

    logger.info(f'Domains = "{domains}"')
    with ThreadPoolExecutor(max_workers=THREADPOOL_SIZE) as executor:
        for future in as_completed(
            executor.submit(
                analyse_domains,
                client=client,
                query=query,
                domains=domains,
                row_index=row_index,
                region_identifier=region,
                top_domains=top_domains,
            )
            for row_index, query
            in enumerate(queries, start=1)
        ):
            row_index, row_content = future.result()

            results[row_index] = row_content

    write_csv(output, results)

    print(f'Results have been successfully written to the file {Path(output).absolute()}')


if __name__ == '__main__':
    main()

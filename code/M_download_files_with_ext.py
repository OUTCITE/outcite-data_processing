import os
import click
import requests
from requests.compat import urljoin
from lxml.html import etree


def extract_links_with_ext(url, extension):
    response = requests.get(url)
    doc_tree = etree.HTML(response.content)
    hrefs = doc_tree.xpath('//a/@href')
    partial_links = [ref for ref in hrefs if ref.endswith(extension)]
    return {urljoin(response.url, ref) for ref in partial_links}


def download_files_from_url(url, extension, output_path):
    """download all files with given ext from url and save to ouput_path"""
    file_links = extract_links_with_ext(url, extension)
    num_links = len(file_links)
    for i, file_link in enumerate(sorted(file_links)):
        file_name = file_link.split('/')[-1]
        file_path = f'{output_path}/{file_name}'
        print(f'Downloading file {i}/{num_links}: {file_link}')

        try:
            if os.path.exists(file_path):
                print(f'{file_path} already downloaded, skipping')
                continue

            res = requests.get(file_link, stream=True)
            if res.status_code != 200:
                err_code = res.status_code
                print(f'could not download {file_link}, error {err_code}')

            with open(file_path, 'wb') as f:
                for chunk in res.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        except Exception as err:
            os.remove(file_path)
            raise err


@click.command('download files with ext')
@click.argument('url', type=click.STRING)
@click.argument('out_path', type=click.Path(file_okay=False, exists=True))
@click.option('-e', '--ext', default='zip', type=click.STRING,
              help='the extension of the file to download (default `zip`)')
def main(url, out_path, ext):
    download_files_from_url(url, ext, out_path)


if __name__ == '__main__':
    main()

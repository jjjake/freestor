#!/usr/bin/env python
"""freestor.py is a script for uploading JSTOR articles to the Internet Archive.
"""
import os

from lxml import etree
import requests
import jinja2
import jsonpatch
import json
# futures is a backport of concurrent.futures (released in python 3.2): 
# https://code.google.com/p/pythonfutures/
import futures

# My a.o functions: https://github.com/jjjake/ia-wrapper.git
import archive


XML_ROOT = '/2/data/jstor/bundle/articles/'
PDF_ROOT = '/2/data/jstor/ejc/jstor-early-journal-content/'


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def pdf_iterator(directory):
    """An iterator that yields file/article level metadata"""
    for root, directories, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            path_details = path.split('/')[-4:]
            md = dict(
                    pdf_path = path,
                    journal = path_details[0],
                    issueid = path_details[1],
                    articleid = '10.2307_{0}'.format(path_details[-1].split('.')[0]),
            )
            xml_path = os.path.join(XML_ROOT, ('{0}.xml'.format(md['articleid'])))
            if os.path.exists(xml_path):
                md['xml_path'] = xml_path
            yield md

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def parse_article_xml(xml_file):
    """Parse a given JSTOR XML file into a Python dictionary."""
    xml = etree.parse(open(xml_file)).getroot().getchildren()
    xml_md = [e for e in xml if e.tag != 'pages']
    md = {}
    for element in xml_md:
        c_elements = element.getchildren()
        if len(c_elements) == 0:
            md[element.tag] = element.text.strip()
            continue
        md[element.tag] = []
        for child in c_elements:
            #if child is None:
            #    continue
            if child.text:
                if child.text.strip() != '':
                    md[element.tag].append(child.text.strip())
            for x in child.getchildren():
                md[element.tag].append({x.tag: x.text.strip()})
    return md

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def make_ia_metadata(file_md, xml_md):
    """Create a metadata dictionary that's ready to hand straight to S3 or the 
    Metadata API.
    """
    md = dict(
        # IA specific
        identifier = 'jstor-{0}'.format(xml_md.get('id').split('/')[-1]),
        mediatype = 'texts',
        publisher = xml_md.get('journaltitle'),
        contributor = 'JSTOR',
        # Throw exception if journalabbrv doesn't exist!
        collection = [
            'jstor_{0}'.format(xml_md['journalabbrv']), 
            'jstor_ejc',
            'additional_collections',
        ],

        date = xml_md.get('pubdate'),
        volume = xml_md.get('volume'),
        pagerange = xml_md.get('pagerange'),
        issn = xml_md.get('issn'),
        source = 'http://www.jstor.org/stable/{0}'.format(xml_md.get('id')),

        article_type = xml_md.get('type'),
        journaltitle = xml_md.get('journaltitle'),
        journalabbrv = xml_md['journalabbrv'],
    )

    # Title ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if xml_md.get('title') is None:
        if '-' not in md['pagerange']:
            md['title'] = '[untitled] {0} ({1}), page {2}'.format(md['journaltitle'], 
                                                                  md['date'],
                                                                  md['pagerange'])
        else:    
            md['title'] = '[untitled] {0}, ({1}), pages {2}'.format(md['journaltitle'],
                                                                    md['date'],
                                                                    md['pagerange'])
    else:
        md['title'] = xml_md['title']

    # Creator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    authors = xml_md.get('authors')
    if authors:
        auth_md = dict((k,v.encode('utf-8')) for d in authors for (k,v) in d.items())
        if auth_md.get('givennames') is not None:
            md['creator'] = '{0}, {1}'.format(auth_md['surname'].strip(' ,'),
                                              auth_md['givennames'].strip(' ,'))
                                              
        elif auth_md.get('stringname'):
            md['creator'] = auth_md['stringname'].strip(' ,')
        elif auth_md.get('surname'):
            md['creator'] = auth_md['surname'].strip(' ,')
        else:
            raise NameError
    else:
        md['creator'] = None

    # Language ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    languages = xml_md.get('languages')
    if languages:
        md['language'] = languages[0].strip()

    # External-identifiers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    external_ids = dict(
            headid = xml_md.get('headid'),
            journalid = xml_md.get('journalid'),
            issueid = xml_md.get('issueid'),
            articleid = xml_md.get('id'),
    )

    md['external-identifier'] = []
    for k,v in external_ids.items():
        if v:
            id = 'urn:jstor-{0}:{1}'.format(k, v)
            md['external-identifier'].append(id)

    # Imagecount ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if xml_md.get('pagerange'):
        try:
            pagerange = xml_md['pagerange'].split('-')[::-1]
            if len(pagerange) == 1:
                imagecount = '2' # Don't forget about the JSTOR cover-page! +1
            else:
                # Add 2 pages to account for the JSTOR cover-page and goofy math.
                imagecount = (int(pagerange[0]) - int(pagerange[1])) + 2
            md['imagecount'] = str(imagecount)
        except ValueError:
            md['imagecount'] = None # TODO: handle this exception better.

    # Generate Description ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    template = jinja2.Template(open('description.html').read())
    md['title'] = unicode(md['title']) # TODO: encode elsewhere...
    md['description'] = template.render(metadata=md).replace('\n', '').strip()

    # Finished ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    return dict((k,v) for k,v in md.items() if v)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def modify_ia_metadata(identifier, metadata={}, target='metadata'):
    """ The IA Metadata API does not yet comply with the latest Json-Patch 
    standard. It currently complies with version 02: 
        https://tools.ietf.org/html/draft-ietf-appsawg-json-patch-02
    The "patch = ..." line is a little hack, for the mean-time, to reformat the
    patch returned by jsonpatch.py (wich complies with version 08).
    """
    log_in_cookies = {'logged-in-sig': os.environ['LOGGED_IN_SIG'],
                      'logged-in-user': os.environ['LOGGED_IN_USER']}
    url = 'http://archive.org/metadata/{0}'.format(identifier)
    src = requests.get(url).json().get(target, {})
    dest = dict((src.items() + metadata.items()))
    json_patch = jsonpatch.make_patch(src, dest).patch
    patch = [{p['op']: p['path'], 'value': p['value']} for p in json_patch] 
    if patch == []:
        return 'No changes made to metadata.'
    params = {'-patch': json.dumps(patch), '-target': target}
    r = requests.patch(url, params=params, cookies=log_in_cookies)
    return r.content

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def upload_article(article):
    xml_md = parse_article_xml(article['xml_path'])
    metadata = make_ia_metadata(article, xml_md)
    files = [article['pdf_path'], article['xml_path']]
    item = archive.Item(metadata['identifier'])
    upload_status = item.upload(files, metadata, 
                                derive=True, 
                                ignore_bucket=False)
    return item.identifier, upload_status

def upload_status(future):
    result = future.result()
    print 'Uploaded:\t{0}'.format(result[0])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    """Concurrently Upload all PDFs that have a matching JSTOR XML file to 
    archive.org
    """
    with futures.ThreadPoolExecutor(max_workers=15) as executor:
        for pdf in pdf_iterator(PDF_ROOT):
            try:
                if not pdf.get('xml_path'):
                    print 'No XML file:\t{0}'.format(pdf.get('articleid'))
                    continue

                # Retry >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                #do = [x.strip() for x in open('itemlist.txt')]
                #pdf_id = pdf['articleid'].split('_')[-1]
                #if not pdf_id in do:
                #    continue
                # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

                # Upload 
                future = executor.submit(upload_article, article=pdf)
                future.add_done_callback(upload_status)

                # METADATA MOD >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                #m = make_ia_metadata(pdf, parse_article_xml(pdf['xml_path']))
                #if m['identifier'] in [x.strip() for x in open('itemlist.txt')]:
                #    print('\n--- {0} ---\n'.format(pdf['xml_path'])
                #    print modify_ia_metadata(m['identifier'], m)
                # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            except KeyboardInterrupt:
                pass

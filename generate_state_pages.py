"""Generate static HTML pages for each USPA location and update sitemap.xml."""

import re
from pathlib import Path
from datetime import date

SCRIPT_DIR = Path(__file__).parent

LOCATIONS = {
    'national':       'National',
    'ipl-world':      'IPL World',
    'alabama':        'Alabama',
    'alaska':         'Alaska',
    'arizona':        'Arizona',
    'arkansas':       'Arkansas',
    'california':     'California',
    'colorado':       'Colorado',
    'connecticut':    'Connecticut',
    'delaware':       'Delaware',
    'florida':        'Florida',
    'georgia':        'Georgia',
    'hawaii':         'Hawaii',
    'idaho':          'Idaho',
    'illinois':       'Illinois',
    'indiana':        'Indiana',
    'iowa':           'Iowa',
    'kansas':         'Kansas',
    'kentucky':       'Kentucky',
    'louisiana':      'Louisiana',
    'maine':          'Maine',
    'maryland':       'Maryland',
    'massachusetts':  'Massachusetts',
    'michigan':       'Michigan',
    'minnesota':      'Minnesota',
    'mississippi':    'Mississippi',
    'missouri':       'Missouri',
    'montana':        'Montana',
    'nebraska':       'Nebraska',
    'nevada':         'Nevada',
    'new-hampshire':  'New Hampshire',
    'new-jersey':     'New Jersey',
    'new-mexico':     'New Mexico',
    'new-york':       'New York',
    'north-carolina': 'North Carolina',
    'north-dakota':   'North Dakota',
    'ohio':           'Ohio',
    'oklahoma':       'Oklahoma',
    'oregon':         'Oregon',
    'pennsylvania':   'Pennsylvania',
    'rhode-island':   'Rhode Island',
    'south-carolina': 'South Carolina',
    'south-dakota':   'South Dakota',
    'tennessee':      'Tennessee',
    'texas':          'Texas',
    'utah':           'Utah',
    'vermont':        'Vermont',
    'virginia':       'Virginia',
    'washington':     'Washington',
    'west-virginia':  'West Virginia',
    'wisconsin':      'Wisconsin',
    'wyoming':        'Wyoming',
}


def make_title(slug, name):
    if slug == 'national':
        return 'USPA National Powerlifting Records | All Divisions & Weight Classes'
    if slug == 'ipl-world':
        return 'IPL World Powerlifting Records | USPA Database'
    return f'{name} USPA Powerlifting Records | State Records Database'


def make_description(slug, name):
    if slug == 'national':
        return ('Browse all USPA national powerlifting records — squat, bench, deadlift, and total '
                'across every division and weight class for drug-tested and non-tested lifters. Updated weekly.')
    if slug == 'ipl-world':
        return ('Browse all IPL World powerlifting records — squat, bench, deadlift, and total '
                'across every division and weight class. Updated weekly from official USPA data.')
    return (f'Browse all {name} USPA powerlifting state records — squat, bench, deadlift, and total '
            f'across every division and weight class for drug-tested and non-tested lifters. Updated weekly.')


def generate_page(slug, name, template):
    canonical = f'https://www.usparecords.com/{slug}/'
    title = make_title(slug, name)
    desc  = make_description(slug, name)

    html = template

    # Head meta replacements
    html = re.sub(r'<title>[^<]*</title>', f'<title>{title}</title>', html)
    html = re.sub(r'<link rel="canonical" href="[^"]*">', f'<link rel="canonical" href="{canonical}">', html)
    html = re.sub(r'<meta name="description" content="[^"]*">', f'<meta name="description" content="{desc}">', html)
    html = re.sub(r'<meta property="og:url" content="[^"]*">', f'<meta property="og:url" content="{canonical}">', html)
    html = re.sub(r'<meta property="og:title" content="[^"]*">', f'<meta property="og:title" content="{title}">', html)
    html = re.sub(r'<meta property="og:description" content="[^"]*">', f'<meta property="og:description" content="{desc}">', html)
    html = re.sub(r'<meta name="twitter:title" content="[^"]*">', f'<meta name="twitter:title" content="{title}">', html)
    html = re.sub(r'<meta name="twitter:description" content="[^"]*">', f'<meta name="twitter:description" content="{desc}">', html)

    # Fix relative paths that break in subdirectories
    html = html.replace('href="uspa medal.png"', 'href="/uspa medal.png"')
    html = html.replace("fetch('last_updated.txt')", "fetch('/last_updated.txt')")

    # Inject preload variable so JS auto-searches this location on load
    html = html.replace('</head>', f'    <script>window._PRELOAD_LOCATION = "{slug}";</script>\n</head>')

    return html


def update_sitemap(today):
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
             f'  <url><loc>https://www.usparecords.com/</loc><lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>1.0</priority></url>']
    for slug in LOCATIONS:
        lines.append(
            f'  <url><loc>https://www.usparecords.com/{slug}/</loc>'
            f'<lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>0.8</priority></url>'
        )
    lines.append('</urlset>')
    (SCRIPT_DIR / 'sitemap.xml').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(f'Updated sitemap.xml ({len(LOCATIONS) + 1} URLs)')


if __name__ == '__main__':
    template = (SCRIPT_DIR / 'index.html').read_text(encoding='utf-8')
    today = date.today().isoformat()

    for slug, name in LOCATIONS.items():
        page_dir = SCRIPT_DIR / slug
        page_dir.mkdir(exist_ok=True)
        html = generate_page(slug, name, template)
        (page_dir / 'index.html').write_text(html, encoding='utf-8')
        print(f'  Generated {slug}/index.html')

    update_sitemap(today)
    print(f'\nDone — {len(LOCATIONS)} state pages generated.')

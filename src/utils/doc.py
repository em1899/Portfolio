
from fpdf import FPDF  # fpdf class
import os
import re
from dotenv import load_dotenv
load_dotenv()   


t1 = ['nb-iot', '4g', 'lte', 'cat-m1', 'esim', 'tracker', 'narrowband',
        'gsm', 'sim', '3g', '2g', 'cellular', 'temperature', 'device']

t4 = ['unlicensed', 'wifi', 'lpwan', 'bluetooth', 'connectivity', 'mesh',
        'lorawan', 'ethernet', 'm-bus', 'zigbee', 'wirepas', 'z-wave', 'bandwidth']


font_resource_path = os.environ['FONT_RESOURCES']


def remove_emoji(name):
    emoji_pattern = re.compile("["
                                    u"\U0001F600-\U0001F64F"  # emoticons
                                    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                    u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                    "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', name)  # no emoji

def generate_doc(companies):

    doc = FPDF(orientation='P', unit='mm', format='A4')

    doc.add_font(
        'DejaVu', '', font_resource_path, uni=True)

    doc.set_margins(20, 20)

    for page, c in enumerate(companies):

        doc.add_page()


        c['company'] = remove_emoji(c['company'])

        # make title
        # doc.set_xy(0.0,0.0)
        doc.set_font('Dejavu', '', 24)
        doc.cell(w=210, h=40, align='L', txt=c['company'], border=0)

        # logging.info domain, rank, score, linkedin
        doc.set_y(50)
        doc.set_font('DejaVu', '', 8)

        if 'http' not in c['domain']:
            company_url = 'https://' + c['domain']
        else:
            company_url= c['domain']

        print(c['linkedin'], company_url, c['score'])

        if c['linkedin'] is None:
            c['linkedin'] = ''

        score = c['score']
        if score is None:
            score = -1000

        doc.multi_cell(w=0, h=6, align='L', txt=c['linkedin'] + '\n' + company_url + '\n' +
                       'Score: ' + str(c['score']) + '\nPages with keywords: ' + '...', border=0)

        # logging.info good stuff
        t1_links =  dict()
        t4_links = dict()
        for term in t1:
            if term in c['term_map']:
                if len(c['term_map'][term]) > 0:
                    t1_links[term] = c['term_map'][term]
        
        for term in t4:
            if term in c['term_map']:
                if len(c['term_map'][term]) > 0:
                    t4_links[term] = c['term_map'][term]

        if len(t1_links) > 0:
            doc.set_font('DejaVu', '', 16)
            doc.multi_cell(w=0, h=10, align='L',
                           txt='The following increased the score', border=0)

        txt = ''
        for g in t1_links:
            txt = ''
            doc.set_font('DejaVu', '', 12)
            doc.multi_cell(w=0, h=8, align='L', txt=g, border=0)

            doc.set_font('DejaVu', '', 8)

            urls = t1_links[g]
            # txt += ': '
            for i, u in enumerate(urls):
                txt += u + '\n'
                if(i > 5):
                    break
            # txt += '\n'

            doc.multi_cell(w=0, h=5, align='L', txt=txt, border=0)

        # logging.info bad stuff
        if len(t4_links) > 0:
            doc.set_font('DejaVu', '', 16)
            doc.multi_cell(w=0, h=10, align='L',
                           txt='The following decreased the score', border=0)

        txt = ''
        for g in t4_links:
            txt = ''
            doc.set_font('DejaVu', '', 12)
            doc.multi_cell(w=0, h=6, align='L', txt=g, border=0)

            doc.set_font('DejaVu', '', 8)

            urls = t4_links[g]
            # txt += ': '
            for i, u in enumerate(urls):
                txt += u + '\n'
                if(i > 5):
                    break
            # txt += '\n'

        doc.multi_cell(w=0, h=5, align='L', txt=txt, border=0)

        # doc.set_font('DejaVu', '', 16)
        # doc.multi_cell(w=0, h=10, align='L',
        #                txt='All matched words', border=0)

        # # # sorted matched words.
        # # matched_sorted = sorted(
        # #     c.matched_words.items(), key=lambda x: x[1], reverse=True)

        
        # # # logging.info all words matched.
        # # txt = ''
        # # for i in matched_sorted:
        # #     txt += i[0] + ':'+str(i[1]) + ', '

        # doc.set_font('DejaVu', '', 8)
        # doc.multi_cell(w=0, h=5, align='L', txt=txt, border=0)

        # logging.info(c.company_name)

    # find available name

    return doc

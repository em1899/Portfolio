import pandas as pd
import decimal
import json

search_map = {"telematics": "telematic", "real time": "real-time", "nb": "nb-iot",
                      "cat m1": "cat-m1", "cat 1": "cat-m1", "ble": "bluetooth", "lte-m": "cat-m1"}

def parse(path):
    df = pd.read_csv(path).drop(
        ['Content Type', 'Status Code', 'Status'], axis=1)
    cols = df.columns.values


    header = cols.tolist()
    header = [column.split(':')[-1].strip().lower()
              for column in header if 'Search' in column]
    features = header.copy()
    header.insert(0, 'address') 

    #rename columns.. 
    df.columns = header
    df = df.loc[:, ~df.columns.duplicated()]  # get rid of duplicates
    #remove sub domains associated with privacy/cookie policies
    df=df[df['address'].str.contains('privacy')==False ]
    df=df[df['address'].str.contains('cookie')==False ]

    # do a bit more of processing.
    df_f = df[features].copy()

    toExport = df_f.copy().sum(axis=0)



    # create a word address mapping... 
    term_domain_mapping = {}
    
    for term in features:
        term_domain_mapping[term] = [x[0] for idx, x in enumerate( df[df[term] > 0][['address']].values.tolist() ) if idx < 5]
        # if len(term_domain_mapping[term]) > 5:
        #     term_domain_mapping[term] = term_domain_mapping[term][0:5]


    
    # bad way to ensure correct parsing of floats.. 
    return json.loads(json.dumps(toExport.to_dict()), parse_float=decimal.Decimal), term_domain_mapping








if __name__ == "__main__":
    a, map = parse('/Users/pb/onomondo/app/onomondo-base/onomondo-crazy/dev/6b8b5919-1df0-49d2-a4ce-8656259c8609/custom_search_all.csv')


    print(a)
    # print(map)
    for el in map:
        print(len(map[el]))
        # print(el)

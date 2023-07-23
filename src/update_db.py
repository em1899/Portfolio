import utils.db as db
import pandas as pd
import sys
import traceback
import getopt


def main(argv):

    inputfile = ''
    try:
        opts, args = getopt.getopt(argv, "hi:")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("update_db.py -i <inputfile>")
            sys.exit()
        elif opt == '-i':
            inputfile = arg

    if inputfile == '':
        print("Export path required")
        sys.exit("Missing path... ")

        # read argument for production

    df = pd.read_csv(inputfile, delimiter=';', keep_default_na=False, usecols=[
                     'Company', 'Class', 'Id', 'Solution'])
    df.columns = df.columns.str.lower()

    df = df.to_dict(orient='records')
    try:
        for el in df:
            el['solution'] = el['solution'].split(',')
    except:
        print(traceback.format_exc())
        sys.exit(1)

    for el in df:
        try:
          db.update(el)
        except:
          print("Failed to update!")
          print(el)
    print('Alles guuuuuuut Andrea')


if __name__ == "__main__":
    main(sys.argv[1:])

#import the chardet library used to figure out the various encodings
import chardet

# function to find the encoding of a file
def find_encoding(fname):
    r_file = open(fname, 'rb').read()
    result = chardet.detect(r_file)
    charenc = result['encoding']
    return charenc
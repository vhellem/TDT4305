

def remove_dollar(line, indeks):
    line[indeks] = float(line[indeks].replace('$', '').replace(',', ''))
    return line


def remove_empty_lines(line, indeks):

    if line[indeks] == '':
        line[indeks] = 0
        return line
    else:
        line[indeks] = float(line[indeks])
        return line



def toCSVLine(data):
    return ','.join(str(d) for d in data)

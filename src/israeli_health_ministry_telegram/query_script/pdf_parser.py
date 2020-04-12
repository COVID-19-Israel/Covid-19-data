from PyPDF4 import PdfFileReader


input = PdfFileReader(open("Actualizacion_54_COVID-19.pdf", "rb"))


content = []
for i in range(input.getNumPages()):
    content.append(input.getPage(i).extractText())

print(type(content[0]))
print(content[0].split())

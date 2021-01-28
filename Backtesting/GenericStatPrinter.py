from openpyxl import Workbook


def print_statistics(results, excel_location):
    wb = Workbook ()

    # grab the active worksheet
    ws = wb.active

    row_count = 1
    for result in results:

        start_char_ascii = 65

        for value in result:
            ws[chr (start_char_ascii) + str (row_count)] = value
            start_char_ascii += 1

        row_count += 1

    wb.save (excel_location)

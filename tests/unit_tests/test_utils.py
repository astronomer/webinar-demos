from include.utils import print_report_row

def test_print_report_row_formats_output(capsys):
    row = (
        1,              # ignored
        "Mars",         # planet
        42,             # passengers
        10,             # active
        32,             # completed
        1234567,        # gross
        12345,          # discount
        1222222,        # net
        "ignored",      # ignored
    )

    print_report_row(row)

    captured = capsys.readouterr()
    output = captured.out.strip()

    assert output == "Mars | 42 | 10 | 32 | 1,234,567 | 12,345 | 1,222,222"

def print_report_row(row):
    (
        _,
        planet,
        passengers,
        active,
        completed,
        gross,
        discount,
        net,
        _,
    ) = row
    print(
        f"{planet} | "
        f"{passengers} | "
        f"{active} | "
        f"{completed} | "
        f"{gross:,} | "
        f"{discount:,} | "
        f"{net:,}"
    )

import random

def get_open_tickets(num: int = 1) -> list[dict]:
    sample_tickets = [
        {
            "ticket_id": "TKT-12345",
            "customer": "john.doe@example.com",
            "subject": "Unable to reset password",
            "message": "I've been trying to reset my password for 2 hours but the reset email never arrives. Can you help?",
            "priority": "high",
        },
        {
            "ticket_id": "TKT-12346",
            "customer": "john.doe@example.com",
            "subject": "Billing discrepancy on latest invoice",
            "message": "Hi, I noticed an extra charge of $49.99 on my latest invoice that I don't recognize. Could you please explain what this charge is for? I haven't made any recent purchases.",
            "priority": "medium",
        },
        {
            "ticket_id": "TKT-12347",
            "customer": "john.doe@example.com",
            "subject": "Feature request: Dark mode",
            "message": "Love your app! Would it be possible to add a dark mode option? I use the app frequently in the evening and it would be much easier on the eyes. Thanks!",
            "priority": "low",
        },
        {
            "ticket_id": "TKT-12348",
            "customer": "john.doe@example.com",
            "subject": "Data export not working",
            "message": "I'm trying to export my data using the Export feature but it keeps failing with an error 'Export timeout'. I have about 3 years of data. Is there a limit or workaround?",
            "priority": "medium",
        },
        {
            "ticket_id": "TKT-12349",
            "customer": "john.doe@example.com",
            "subject": "Account locked after failed login attempts",
            "message": "My account got locked after I entered the wrong password a few times. I know my correct password now but can't access my account. How can I unlock it?",
            "priority": "high",
        },
    ]

    return random.sample(sample_tickets, num)

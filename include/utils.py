import random

def get_info_for_high_value_users():
    names = [
        "Alex", "Jordan", "Taylor", "Morgan", "Casey",
        "Riley", "Jamie", "Avery", "Drew", "Skyler",
        "Cameron", "Peyton", "Quinn", "Reese", "Hayden"
    ]
    users = [
        {
            "user_id": 1,
            "mood_score": 98,
            "recent_bets": [
                {
                    "type": "soccer",
                    "outcome": "won",
                    "amount_bet": 200,
                    "amount_won": 4000,
                    "timestamp": "2025-05-01T15:30:00Z"
                },
                {
                    "type": "basketball",
                    "outcome": "lost",
                    "amount_bet": 150,
                    "amount_won": 0,
                    "timestamp": "2025-05-10T17:00:00Z"
                },
                {
                    "type": "soccer",
                    "outcome": "won",
                    "amount_bet": 250,
                    "amount_won": 5000,
                    "timestamp": "2025-05-15T19:45:00Z"
                }
            ],
            "favorite_sports": ["soccer", "basketball"],
            "preferred_bet_types": ["parlay", "moneyline"],
            "average_bet_amount": 150,
            "lifetime_winnings": 12000,
            "last_login": "2025-05-01T16:00:00Z",
            "location": "California",
            "language_preference": "en",
            "risk_profile": "high",
            "engagement_score": 87,
            "favorite_team": "LA Galaxy"
        },
        {
            "user_id": 2,
            "mood_score": 85,
            "recent_bets": [
                {
                    "type": "basketball",
                    "outcome": "lost",
                    "amount_bet": 100,
                    "amount_won": 0,
                    "timestamp": "2025-05-09T18:00:00Z"
                },
                {
                    "type": "tennis",
                    "outcome": "won",
                    "amount_bet": 120,
                    "amount_won": 600,
                    "timestamp": "2025-05-12T14:00:00Z"
                },
                {
                    "type": "basketball",
                    "outcome": "won",
                    "amount_bet": 130,
                    "amount_won": 260,
                    "timestamp": "2025-05-18T20:30:00Z"
                }
            ],
            "favorite_sports": ["basketball", "tennis"],
            "preferred_bet_types": ["spread", "over/under"],
            "average_bet_amount": 120,
            "lifetime_winnings": 8000,
            "last_login": "2025-05-09T18:30:00Z",
            "location": "New York",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 75,
            "favorite_team": "New York Knicks"
        },
        {
            "user_id": 3,
            "mood_score": 92,
            "recent_bets": [
                {
                    "type": "tennis",
                    "outcome": "won",
                    "amount_bet": 250,
                    "amount_won": 1000,
                    "timestamp": "2025-05-08T14:20:00Z"
                },
                {
                    "type": "golf",
                    "outcome": "lost",
                    "amount_bet": 200,
                    "amount_won": 0,
                    "timestamp": "2025-05-11T13:10:00Z"
                },
                {
                    "type": "tennis",
                    "outcome": "won",
                    "amount_bet": 180,
                    "amount_won": 720,
                    "timestamp": "2025-05-16T16:00:00Z"
                }
            ],
            "favorite_sports": ["tennis", "golf"],
            "preferred_bet_types": ["moneyline"],
            "average_bet_amount": 200,
            "lifetime_winnings": 9500,
            "last_login": "2025-05-08T15:00:00Z",
            "location": "Florida",
            "language_preference": "es",
            "risk_profile": "high",
            "engagement_score": 80,
            "favorite_team": "Miami Heat"
        },
        {
            "user_id": 4,
            "mood_score": 77,
            "recent_bets": [
                {
                    "type": "baseball",
                    "outcome": "won",
                    "amount_bet": 300,
                    "amount_won": 900,
                    "timestamp": "2025-05-07T17:45:00Z"
                },
                {
                    "type": "football",
                    "outcome": "lost",
                    "amount_bet": 180,
                    "amount_won": 0,
                    "timestamp": "2025-05-13T19:00:00Z"
                },
                {
                    "type": "baseball",
                    "outcome": "won",
                    "amount_bet": 200,
                    "amount_won": 600,
                    "timestamp": "2025-05-19T21:15:00Z"
                }
            ],
            "favorite_sports": ["baseball", "football"],
            "preferred_bet_types": ["parlay"],
            "average_bet_amount": 180,
            "lifetime_winnings": 7000,
            "last_login": "2025-05-07T18:00:00Z",
            "location": "Texas",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 70,
            "favorite_team": "Houston Astros"
        },
        {
            "user_id": 5,
            "mood_score": 88,
            "recent_bets": [
                {
                    "type": "football",
                    "outcome": "lost",
                    "amount_bet": 400,
                    "amount_won": 0,
                    "timestamp": "2025-05-06T21:10:00Z"
                },
                {
                    "type": "soccer",
                    "outcome": "won",
                    "amount_bet": 220,
                    "amount_won": 880,
                    "timestamp": "2025-05-14T18:40:00Z"
                },
                {
                    "type": "football",
                    "outcome": "won",
                    "amount_bet": 300,
                    "amount_won": 1200,
                    "timestamp": "2025-05-20T22:00:00Z"
                }
            ],
            "favorite_sports": ["football", "soccer"],
            "preferred_bet_types": ["moneyline", "spread"],
            "average_bet_amount": 220,
            "lifetime_winnings": 11000,
            "last_login": "2025-05-06T21:30:00Z",
            "location": "Nevada",
            "language_preference": "en",
            "risk_profile": "high",
            "engagement_score": 90,
            "favorite_team": "Las Vegas Raiders"
        },
        {
            "user_id": 6,
            "mood_score": 80,
            "recent_bets": [
                {
                    "type": "golf",
                    "outcome": "won",
                    "amount_bet": 150,
                    "amount_won": 600,
                    "timestamp": "2025-05-05T13:00:00Z"
                },
                {
                    "type": "tennis",
                    "outcome": "lost",
                    "amount_bet": 140,
                    "amount_won": 0,
                    "timestamp": "2025-05-13T15:30:00Z"
                },
                {
                    "type": "golf",
                    "outcome": "won",
                    "amount_bet": 160,
                    "amount_won": 640,
                    "timestamp": "2025-05-17T12:00:00Z"
                }
            ],
            "favorite_sports": ["golf", "tennis"],
            "preferred_bet_types": ["over/under"],
            "average_bet_amount": 140,
            "lifetime_winnings": 6000,
            "last_login": "2025-05-05T13:30:00Z",
            "location": "Arizona",
            "language_preference": "en",
            "risk_profile": "low",
            "engagement_score": 65,
            "favorite_team": "Arizona Cardinals"
        },
        {
            "user_id": 7,
            "mood_score": 95,
            "recent_bets": [
                {
                    "type": "soccer",
                    "outcome": "won",
                    "amount_bet": 500,
                    "amount_won": 2500,
                    "timestamp": "2025-05-04T19:30:00Z"
                },
                {
                    "type": "basketball",
                    "outcome": "won",
                    "amount_bet": 320,
                    "amount_won": 960,
                    "timestamp": "2025-05-11T21:00:00Z"
                },
                {
                    "type": "soccer",
                    "outcome": "lost",
                    "amount_bet": 280,
                    "amount_won": 0,
                    "timestamp": "2025-05-18T18:15:00Z"
                }
            ],
            "favorite_sports": ["soccer", "basketball"],
            "preferred_bet_types": ["parlay", "moneyline"],
            "average_bet_amount": 300,
            "lifetime_winnings": 15000,
            "last_login": "2025-05-04T20:00:00Z",
            "location": "Illinois",
            "language_preference": "en",
            "risk_profile": "high",
            "engagement_score": 93,
            "favorite_team": "Chicago Bulls"
        },
        {
            "user_id": 8,
            "mood_score": 83,
            "recent_bets": [
                {
                    "type": "basketball",
                    "outcome": "lost",
                    "amount_bet": 180,
                    "amount_won": 0,
                    "timestamp": "2025-05-03T20:00:00Z"
                },
                {
                    "type": "baseball",
                    "outcome": "won",
                    "amount_bet": 160,
                    "amount_won": 480,
                    "timestamp": "2025-05-12T17:30:00Z"
                },
                {
                    "type": "basketball",
                    "outcome": "won",
                    "amount_bet": 170,
                    "amount_won": 340,
                    "timestamp": "2025-05-21T19:00:00Z"
                }
            ],
            "favorite_sports": ["basketball", "baseball"],
            "preferred_bet_types": ["spread"],
            "average_bet_amount": 160,
            "lifetime_winnings": 7200,
            "last_login": "2025-05-03T20:30:00Z",
            "location": "Ohio",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 72,
            "favorite_team": "Cleveland Cavaliers"
        },
        {
            "user_id": 9,
            "mood_score": 90,
            "recent_bets": [
                {
                    "type": "football",
                    "outcome": "won",
                    "amount_bet": 350,
                    "amount_won": 1400,
                    "timestamp": "2025-05-02T22:10:00Z"
                },
                {
                    "type": "golf",
                    "outcome": "lost",
                    "amount_bet": 210,
                    "amount_won": 0,
                    "timestamp": "2025-05-13T11:00:00Z"
                },
                {
                    "type": "football",
                    "outcome": "won",
                    "amount_bet": 250,
                    "amount_won": 1000,
                    "timestamp": "2025-05-22T20:00:00Z"
                }
            ],
            "favorite_sports": ["football", "golf"],
            "preferred_bet_types": ["moneyline", "over/under"],
            "average_bet_amount": 210,
            "lifetime_winnings": 10500,
            "last_login": "2025-05-02T22:30:00Z",
            "location": "Georgia",
            "language_preference": "en",
            "risk_profile": "high",
            "engagement_score": 85,
            "favorite_team": "Atlanta Falcons"
        },
        {
            "user_id": 10,
            "mood_score": 82,
            "recent_bets": [
                {
                    "type": "tennis",
                    "outcome": "lost",
                    "amount_bet": 220,
                    "amount_won": 0,
                    "timestamp": "2025-05-01T16:40:00Z"
                },
                {
                    "type": "soccer",
                    "outcome": "won",
                    "amount_bet": 170,
                    "amount_won": 510,
                    "timestamp": "2025-05-14T18:00:00Z"
                },
                {
                    "type": "tennis",
                    "outcome": "won",
                    "amount_bet": 180,
                    "amount_won": 720,
                    "timestamp": "2025-05-23T15:00:00Z"
                }
            ],
            "favorite_sports": ["tennis", "soccer"],
            "preferred_bet_types": ["spread", "parlay"],
            "average_bet_amount": 170,
            "lifetime_winnings": 6800,
            "last_login": "2025-05-01T17:00:00Z",
            "location": "Washington",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 68,
            "favorite_team": "Seattle Sounders"
        }
    ]
    random.shuffle(names)
    for i, user in enumerate(users):
        user["user_name"] = names[i % len(names)]
    return users


import random

def get_random_user_profile():
    profiles = [
        {
            "user_id": 1,
            "mood_score": 95,
            "recent_bets": [
                {
                    "type": "football",
                    "outcome": "won",
                    "amount_bet": 500,
                    "amount_won": 2500,
                    "timestamp": "2025-05-01T20:00:00Z"
                },
                {
                    "type": "basketball",
                    "outcome": "won",
                    "amount_bet": 300,
                    "amount_won": 1500,
                    "timestamp": "2025-05-05T21:00:00Z"
                }
            ],
            "favorite_sports": ["football", "basketball"],
            "preferred_bet_types": ["spread", "moneyline"],
            "average_bet_amount": 400,
            "lifetime_winnings": 200000,
            "last_login": "2025-05-25T22:00:00Z",
            "location": "California",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 90,
            "favorite_team": "Golden State Warriors"
        },
        {
            "user_id": 2,
            "mood_score": 45,
            "recent_bets": [
                {
                    "type": "golf",
                    "outcome": "lost",
                    "amount_bet": 200,
                    "amount_won": 0,
                    "timestamp": "2025-05-03T10:00:00Z"
                },
                {
                    "type": "soccer",
                    "outcome": "lost",
                    "amount_bet": 300,
                    "amount_won": 0,
                    "timestamp": "2025-05-06T16:00:00Z"
                }
            ],
            "favorite_sports": ["golf", "soccer"],
            "preferred_bet_types": ["moneyline"],
            "average_bet_amount": 250,
            "lifetime_winnings": 0,
            "last_login": "2025-05-24T15:00:00Z",
            "location": "Nevada",
            "language_preference": "en",
            "risk_profile": "low",
            "engagement_score": 40,
            "favorite_team": "Las Vegas Raiders"
        },
        {
            "user_id": 3,
            "mood_score": 80,
            "recent_bets": [
                {
                    "type": "horse racing",
                    "outcome": "won",
                    "amount_bet": 1000,
                    "amount_won": 5000,
                    "timestamp": "2025-05-10T14:00:00Z"
                },
                {
                    "type": "football",
                    "outcome": "lost",
                    "amount_bet": 1200,
                    "amount_won": 0,
                    "timestamp": "2025-05-20T20:00:00Z"
                }
            ],
            "favorite_sports": ["horse racing", "football"],
            "preferred_bet_types": ["parlay", "moneyline"],
            "average_bet_amount": 1100,
            "lifetime_winnings": 20000,
            "last_login": "2025-05-26T21:30:00Z",
            "location": "Texas",
            "language_preference": "en",
            "risk_profile": "high",
            "engagement_score": 95,
            "favorite_team": "Dallas Cowboys"
        },
        {
            "user_id": 4,
            "mood_score": 70,
            "recent_bets": [
                {
                    "type": "basketball",
                    "outcome": "lost",
                    "amount_bet": 150,
                    "amount_won": 0,
                    "timestamp": "2025-05-08T19:00:00Z"
                },
                {
                    "type": "tennis",
                    "outcome": "won",
                    "amount_bet": 200,
                    "amount_won": 600,
                    "timestamp": "2025-05-12T17:00:00Z"
                }
            ],
            "favorite_sports": ["basketball", "tennis"],
            "preferred_bet_types": ["over/under"],
            "average_bet_amount": 175,
            "lifetime_winnings": 5000,
            "last_login": "2025-05-25T19:00:00Z",
            "location": "Florida",
            "language_preference": "en",
            "risk_profile": "medium",
            "engagement_score": 70,
            "favorite_team": "Miami Heat"
        }
    ]
    return random.choice(profiles)

def post_message_ui(user_info=None, queue_url=None):
    pass

# # function that creates a csv from the user data
# def create_user_csv(users, file_path="high_value_users.csv"):
#     import pandas as pd
#     df = pd.DataFrame(users)
#     df.to_csv(file_path, index=False)
#     print(f"CSV file created at {file_path}")


# if __name__ == "__main__":
#     create_user_csv(get_info_for_high_value_users(), "high_value_users.csv")
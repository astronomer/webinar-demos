import pandas as pd
import numpy as np
from faker import Faker
import random
import uuid

fake = Faker()


def generate_uuids(n):
    return [str(uuid.uuid4()) for _ in range(n)]


def generate_users_data(num_users=100, date=None, filename="users.csv"):
    data = {
        "user_id": generate_uuids(num_users),
        "user_name": [fake.name() for _ in range(num_users)],
        "date_of_birth": [
            fake.date_of_birth(minimum_age=18, maximum_age=70) for _ in range(num_users)
        ],
        "sign_up_date": [
            fake.date_between(start_date="-3y", end_date="today")
            for _ in range(num_users)
        ],
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_teas_data(filename="teas.csv", date=None):

    uuids = [
        "550e8400-e29b-41d4-a716-446655440000",
        "6fa459ea-ee8a-3ca4-894e-db77e160355e",
        "7d44a4d3-c8c8-45dd-bdc6-2222a56e7f5f",
        "123e4567-e89b-12d3-a456-426614174000",
        "e7fe1d3d-bd91-4f3d-8c26-e0cbe8ed3d6c",
        "34b34f28-afe0-4e78-80a1-8c9390c61dc6",
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "1e1cfbd8-1b1b-4b2c-9c3f-7f57c51925b1",
        "de305d54-75b4-431b-adb2-eb6b9e546014",
        "7b50f5c8-44e3-45a7-919e-dac4905b8d9c",
        "5a6b8baf-0f8d-4cc8-bf08-d85a89e8e5f3",
        "69e6b36c-3f72-4c4c-945c-1b0d9e8c2e9a",
        "b6f55d32-36db-4092-80e4-43b8b7b7c9ff",
        "348f1f5e-282d-4f07-ae7a-518a2b682dee",
        "7c7b0af2-9234-4c99-9b8b-7d0b4a9f7a58",
        "953b7c72-5e84-4d3e-b3e7-c6b24d9c59ea",
        "814b9d82-3769-45b3-a36e-5f3d7a5bbf7f",
        "92c1683a-1b84-4626-a807-afe73cdabc8b",
        "95c1063f-17d8-429b-97c2-3c7b59d7e89b",
        "816c5a91-1a24-4c26-8c3a-2b7b1bda73a5",
        "ee59c7f6-4a78-4cb9-82c7-4b84e7aebf4f",
        "4f0f81c7-7f54-4f07-8101-bb2c790c8c1b",
        "6311c573-40f7-47f9-9d2e-b74c2a4a8c8f",
        "4f63e6b9-4d1d-491d-bc5f-3e1b1f6f4e6d",
        "2a4df7d8-335c-4d8b-8d6c-7d43b4b5f1f9",
        "9b9f0c2f-0d94-4e4e-934c-b5f3a6a87e59",
        "ce47f814-6e25-4965-8f8c-97d487f8a7c7",
        "3b6d2c17-4129-4a56-8f9b-7b2d7e8f4e4b",
        "bc2e597a-4cf3-46b3-8f9a-61f7e7d9a2c8",
        "f6b89c3e-5e6f-487d-9e4b-2e9c7e8f4c6b",
    ]

    tea_names = [
        "Sencha",
        "Matcha",
        "Dragonwell",
        "Gunpowder",
        "Gyokuro",
        "Bancha",
        "Darjeeling",
        "Assam",
        "Ceylon",
        "Lapsang Souchong",
        "Keemun",
        "Yunnan Black",
        "Earl Grey",
        "English Breakfast",
        "Chai",
        "Masala Chai",
        "Rooibos Chai",
        "Herbal Mint",
        "Chamomile",
        "Lemon Verbena",
        "Peppermint",
        "Hibiscus",
        "Jasmine",
        "Osmanthus",
        "Rose",
        "Oolong",
        "Tieguanyin",
        "Da Hong Pao",
        "Rooibos",
        "Honeybush",
    ]
    tea_types = [
        "Green",
        "Green",
        "Green",
        "Green",
        "Green",
        "Green",
        "Black",
        "Black",
        "Black",
        "Black",
        "Black",
        "Black",
        "Black",
        "Black",
        "Chai",
        "Chai",
        "Chai",
        "Herbal",
        "Herbal",
        "Herbal",
        "Herbal",
        "Herbal",
        "Floral",
        "Floral",
        "Floral",
        "Oolong",
        "Oolong",
        "Oolong",
        "Herbal",
        "Herbal",
    ]
    prices = np.round(np.random.uniform(5.0, 20.0, size=len(tea_names)), 2)

    data = {
        "tea_id": uuids,
        "tea_name": tea_names,
        "tea_type": tea_types,
        "price": prices,
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_utm_data(num_utms=100, date=None, filename="utms.csv"):
    utm_sources = ["Google", "Facebook", "Twitter", "LinkedIn", "Instagram"]
    utm_mediums = ["CPC", "email", "social", "referral", "banner"]
    utm_campaigns = [
        "Summer_Sale",
        "Winter_Discount",
        "Black_Friday",
        "Holiday_Special",
        "Spring_Promo",
    ]
    utm_terms = ["discount", "sale", "promotion", "deal", "offer"]
    utm_contents = ["image1", "image2", "text_link", "button_click", None]

    data = {
        "utm_id": generate_uuids(num_utms),
        "utm_source": random.choices(utm_sources, k=num_utms),
        "utm_medium": random.choices(utm_mediums, k=num_utms),
        "utm_campaign": random.choices(utm_campaigns, k=num_utms),
        "utm_term": random.choices(utm_terms, k=num_utms),
        "utm_content": random.choices(utm_contents, k=num_utms),
        "updated_at": date,
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def generate_sales_data(
    num_sales=1000, users_df=None, teas_df=None, utm_df=None, filename="sales.csv", date=None
):
    if num_sales == 1:
        data = {
            "sale_id": generate_uuids(num_sales),
            "user_id": users_df["user_id"].values[0],
            "tea_id": teas_df["tea_id"].values[0],
            "utm_id": utm_df["utm_id"].values[0],
            "quantity": np.random.randint(1, 10),
            "sale_date": date,
        }

    elif num_sales > 1:
        data = {
            "sale_id": generate_uuids(num_sales),
            "user_id": np.random.choice(users_df["user_id"], size=num_sales),
            "tea_id": np.random.choice(teas_df["tea_id"], size=num_sales),
            "utm_id": np.random.choice(utm_df["utm_id"], size=num_sales),
            "quantity": np.random.randint(1, 10, size=num_sales),
            "sale_date": date,
        }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    return df


def get_new_sales_from_internal_api(num_sales, date):
    num_users = int(0.75 * num_sales)
    if num_users < 1:
        num_users = 1
    users_df = generate_users_data(num_users=num_users, date=date)
    teas_df = generate_teas_data(date=date)
    utm_df = generate_utm_data(num_utms=num_users, date=date)
    sales_df = generate_sales_data(
        num_sales=num_sales, users_df=users_df, teas_df=teas_df, utm_df=utm_df, date=date
    )
    return sales_df, users_df, teas_df, utm_df

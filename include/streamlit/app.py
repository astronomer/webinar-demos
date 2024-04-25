import streamlit as st
import weaviate
import json
from openai import OpenAI
import os

WEAVIATE_CLASS_NAME = "KB"


def get_embedding(text):
    model = "text-embedding-ada-002"
    client = OpenAI()
    embeddings = client.embeddings.create(input=[text], model=model).data

    return [x.embedding for x in embeddings]


def get_relevant_articles(reworded_prompt, limit=5, certainty=0.75):

    client = weaviate.Client(
        url="http://weaviate:8081",
        auth_client_secret=weaviate.AuthApiKey("adminkey"),
    )

    input_text = reworded_prompt

    nearVector = get_embedding(input_text)

    count = client.query.get(WEAVIATE_CLASS_NAME, ["full_text"]).do()
    st.write(f"Total Info Chunks: {len(count['data']['Get'][WEAVIATE_CLASS_NAME])}")

    result = (
        client.query.get(
            WEAVIATE_CLASS_NAME, ["title", "uri", "full_text", "chunk_index"]
        )
        .with_near_vector({"vector": nearVector[0], "certainty": certainty})
        .with_limit(limit)
        .do()
    )

    return result["data"]["Get"][WEAVIATE_CLASS_NAME]


def get_response(articles, query):
    prompt = """You are the helpful social post generator Astra! You will create an interesting factoid post 
    about Airflow and the topic requested by the user:"""

    for article in articles:
        article_title = article["title"] if article["title"] else "unknown"

        article_full_text = article["full_text"] if article["full_text"] else "no text"

        article_info = article_title + " Full text: " + article_full_text
        prompt += " " + article_info + " "

    prompt += "Your user asks:"

    prompt += " " + query

    prompt += """ 
    Remember to keep the post short and sweet! At the end of the post add another sentence that is a space fact!"""

    client = OpenAI()

    if os.path.exists("/app/include/model_results/champion/champion_accuracy.json"):
        with open(
            "/app/include/model_results/champion/champion_accuracy.json", "r"
        ) as f:
            champion_model_id = json.load(f)["challenger_model_id"]
    else:
        champion_model_id = "gpt-3.5-turbo"

    chat_completion = client.chat.completions.create(
        model=champion_model_id, messages=[{"role": "user", "content": prompt}]
    )

    return chat_completion


def get_image(response_text):
    from openai import OpenAI

    client = OpenAI()

    r = client.images.generate(
        model="dall-e-3",
        prompt="Create an image to go along with the following social media post:" + response_text,
        n=1,
        size="1024x1024",
    )

    image_url = r.data[0].url

    return image_url


# Streamlit app code
st.title("Create social media posts with Astra!")

st.header("Search")

user_input = st.text_input(
    "Your post idea:",
    "Create a LinkedIn post for me about dynamic task mapping!",
)
limit = st.slider("Retrieve X most relevant chunks:", 1, 20, 5)
certainty = st.slider("Certainty threshold for relevancy", 0.0, 1.0, 0.75)

if "response_text" not in st.session_state:
    st.session_state["response_text"] = ""

if "picture" not in st.session_state:
    st.session_state["picture"] = ""

if "articles" not in st.session_state:
    st.session_state["articles"] = []

if st.button("Generate post!"):
    st.header("Answer")
    with st.spinner(text="Thinking... :thinking_face:"):
        articles = get_relevant_articles(user_input, limit=limit, certainty=certainty)
        response = get_response(articles=articles, query=user_input)
        st.session_state["response_text"] = response.choices[0].message.content
        st.session_state["articles"] = articles

        st.success("Done! :smile:")

if st.session_state["response_text"]:
    st.write(st.session_state["response_text"])

    if st.button("Generate picture"):
        with st.spinner(text="Generating... :camera:"):
            st.session_state["picture"] = get_image(st.session_state["response_text"])

if st.session_state["picture"]:
    st.image(st.session_state["picture"], caption="DALLE-3 generated image")

if st.session_state["response_text"]:
    st.header("Sources")

    for article in st.session_state["articles"]:
        st.write(f"URI: {article['uri']}")
        st.write(f"Chunk number: {article['chunk_index']}")
        st.write("---")

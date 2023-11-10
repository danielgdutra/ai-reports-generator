import base64, os
import openai


def generate_insights(data):
    client = openai.OpenAI(api_key=base64.b64decode(os.getenv('CHATGPT_API_KEY')).decode('utf-8'))

    chat_completion = (client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that analyses data to provide insights about job advertising"
                           " data for clients aiming to reduce the cost per apply and increase the number of applies."
            },
            {
                "role": "user",
                "content": f"Here is a summary of the client's data report: {data}\n\nProvide 10 useful insights:",
            }
        ],
        model="gpt-3.5-turbo",
    ))

    insights = chat_completion.choices[0].text.strip()
    return insights

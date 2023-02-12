# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://gallardorivilla.es/wp-content/uploads/2023/02/CHAT-GPT3.png" alt="Chat GPT3" >
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Install Open AI and Token Counter 🪄

# COMMAND ----------

pip install openai

# COMMAND ----------

pip install tiktoken

# COMMAND ----------

# MAGIC %md
# MAGIC For English text, 1 token is approximately 4 characters or 0.75 words.

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 Tokenizer tool --> https://platform.openai.com/tokenizer

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 FAQ Tokens --> https://openai.com/api/pricing/#faq-token

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Get API Key 🔒 and config Key Vault 🗝️

# COMMAND ----------

# MAGIC %md
# MAGIC URL --> https://platform.openai.com/account/api-keys

# COMMAND ----------

# MAGIC %md
# MAGIC Change your KeyVault --> dbutils.secrets.get(scope="KeyVault",key="GPT3")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Run script ✨

# COMMAND ----------

import openai
import tiktoken

def options():
    print("-----------------------------------")
    print("Option 1: Question  CHAT GPT3.")
    print("Option 2: Get tokens numbers")
    print("Option 3: Enter other number o any to terminate")
    print("-----------------------------------")

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return print("Total tokens number:",num_tokens)

def gpt3_completion(prompt, engine='text-davinci-003', temp=0, tokens=2048, top_p=1.0, freq_pen=0.0, pres_pen=0.0,stop=['"\"\"\""']):
    try:
        openai.api_key = dbutils.secrets.get(scope="KeyVault",key="GPT3")    
        prompt = prompt.encode(encoding='ASCII',errors='ignore').decode()
        completion = openai.Completion.create(
            engine=engine,
            prompt=prompt,
            temperature=temp,
            max_tokens=tokens,
            top_p=top_p,
            frequency_penalty=freq_pen,
            presence_penalty=pres_pen,
            stop=stop)
        text = completion.choices[0].text
    except Exception as error:
        print(f"Error description : {error}")
    return text

while True:
    options()
    option = int(input())
    if option == 1:
        question = input("Write you answer here:")
        response = gpt3_completion(question)
        print(response)
    elif option == 2:
        question = input("Write you answer here:")
        num_tokens_from_string(question, "cl100k_base")
    else:
        print("----")
        print("Bye")
        print("----")
        break

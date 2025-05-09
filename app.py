import streamlit as st 
from lida import Manager, TextGenerationConfig , llm  
from dotenv import load_dotenv
import os
import openai
from PIL import Image
from io import BytesIO
import base64

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

def base64_to_image(base64_string):
    # Decode the base64 string
    byte_data = base64.b64decode(base64_string)
    
    # Use BytesIO to convert the byte data to image
    return Image.open(BytesIO(byte_data))


lida = Manager(text_gen = llm("openai"))
textgen_config = TextGenerationConfig(n=1, temperature=0.5, model="gpt-3.5-turbo", use_cache=True)
# textgen_config = TextGenerationConfig(n=1, temperature=0.5, model="o4-mini-2025-04-16", use_cache=True)

menu = st.sidebar.selectbox("Choose an Option", ["Summarize", "Question based Graph"])



if menu == "Summarize":
    st.subheader("Summarization of your Data")
    file_uploader = st.file_uploader("Upload your CSV", type="csv")
    
    if file_uploader is not None:
        path_to_save = "filename.csv"
        with open(path_to_save, "wb") as f:
            f.write(file_uploader.getvalue())

        summary = lida.summarize("filename.csv", summary_method="default", 
        textgen_config=textgen_config)
        st.write(summary)
        

        all_goals = lida.goals(summary, n=10, textgen_config=textgen_config)

        library = "seaborn"
        textgen_config = TextGenerationConfig(n=4, temperature=0.2, use_cache=True)

        chart_count = 0
        for i, goal in enumerate(all_goals):
            if chart_count >= 4:
                break

            charts = lida.visualize(summary=summary, goal=goal, textgen_config=textgen_config, library=library)

            if charts and charts[0].raster:
                st.write(f"Goal {chart_count + 1}: {goal}")
                img = base64_to_image(charts[0].raster)
                st.image(img)
                chart_count += 1
        
        # for i in range(len(charts)):
        #     st.write(len(charts))
        #     st.write(f"LOOPING {i}")
        #     if charts[i]:
        #         img_base64_string = charts[i].raster
        #         img = base64_to_image(img_base64_string)
        #         st.image(img)
        #     else:
        #         st.write("LALALALA")
        


        
elif menu == "Question based Graph":
    st.subheader("Query your Data to Generate Graph")
    file_uploader = st.file_uploader("Upload your CSV", type="csv")
    if file_uploader is not None:
        path_to_save = "filename1.csv"
        with open(path_to_save, "wb") as f:
            f.write(file_uploader.getvalue())
        text_area = st.text_area("Query your Data to Generate Graph", height=200)
        if st.button("Generate Graph"):
            if len(text_area) > 0:
                st.info("Your Query: " + text_area)
                lida = Manager(text_gen = llm("openai")) 
                textgen_config = TextGenerationConfig(n=1, temperature=0.2, use_cache=True)
                summary = lida.summarize("filename1.csv", summary_method="llm", textgen_config=textgen_config)
                user_query = text_area
                charts = lida.visualize(summary=summary, goal=user_query, textgen_config=textgen_config)  
                charts[0]
                image_base64 = charts[0].raster
                img = base64_to_image(image_base64)
                st.image(img)
            





FROM python:3.9-slim

WORKDIR /src/app

COPY . ./src/app

RUN pip install -r ./src/app/requirements.txt

EXPOSE 8501

ENTRYPOINT [ "streamlit", "run" ] 
CMD [ "./src/app/main.py" ]
FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./

RUN python3.11 -m pip install -r requirements.txt -t .
RUN python3.11 -m spacy download en_core_web_sm 
#RUN python3.11 -m nltk.downloader punkt punkt_tab stopwords -d /root/nltk_data
#ENV NLTK_DATA=/root/nltk_data

COPY app.py ./

# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]

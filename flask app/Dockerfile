FROM python:buster
COPY . /app
COPY templates templates
EXPOSE 9090
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
#RUN export FLASK_APP=app.py
#CMD ["flask", "run", "--host", "0.0.0.0"]
CMD ["flaskapp.py"]
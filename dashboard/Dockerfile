FROM python:3.6-slim

RUN pip install dash==0.31.1
RUN pip install dash-html-components==0.13.2  
RUN pip install dash-core-components==0.38.1 
RUN pip install dash-table==3.1.7  
RUN pip install pydrill
RUN pip install jinja2
RUN pip install requests
RUN pip install gevent

EXPOSE 8050
#CMD ["gunicorn","--workers=2","~/dashboard/server/app:server"]
CMD ["python","/dashboard/server/app.py"]

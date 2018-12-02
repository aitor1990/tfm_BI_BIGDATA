FROM python:3.6-slim

RUN pip install dash==0.31.1
RUN pip install dash-html-components==0.13.2  
RUN pip install dash-core-components==0.38.1 
RUN pip install dash-table==3.1.7  
EXPOSE 8050
ADD /server/app.py app.py
CMD ["python","app.py"]

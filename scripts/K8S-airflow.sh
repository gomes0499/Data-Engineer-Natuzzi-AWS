helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace

kubectl port-forward --namespace airflow svc/airflow-web 8080:8080



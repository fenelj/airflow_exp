FROM apache/airflow:3.2.2-python3.14

# Install necessary providers and dependencies into the image
# This ensures fast startup on ECS since packages don't need to be downloaded at runtime.
# Amazon and Bash are already included.
# Adding NiFi provider as requested:
# NiFi provider not available for Python 3.12/3.14

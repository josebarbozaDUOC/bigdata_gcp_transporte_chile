# Usa una imagen base oficial de Python
FROM python:3.8-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY main.py requirements.txt ./

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Define el puerto que usará la aplicación
ENV PORT 8080

# Expone el puerto
EXPOSE 8080

# Define el punto de entrada del contenedor
CMD ["python", "main.py"]

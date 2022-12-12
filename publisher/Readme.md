# Build and deploy to K8s
1. az login --use-device-code
2. az acr login --name natshelmacr
3. If you have a previous instance running in k8s stop it
    ```
        kubectl delete -f publisher-service.yaml
    ```
4. Update the version of image in publisher-service.yaml. Update the number of replicas if needed.
5. Build and tag the service
    ```
        docker build -t natshelmacr.azurecr.io/go-buffer-publish:<version> .
    ```
6. Push the image to ACR
    ```
        docker push natshelmacr.azurecr.io/go-buffer-publish:<version>
    ```
7. Deploy the service
    ```
        kubectl apply -f publisher-service.yaml
    ```

## **Note**

The stream lives and dies with with the publisher service, so if publisher service is stopped, consumer service will break. ALways restart the consumer service when publisher service is restarted.
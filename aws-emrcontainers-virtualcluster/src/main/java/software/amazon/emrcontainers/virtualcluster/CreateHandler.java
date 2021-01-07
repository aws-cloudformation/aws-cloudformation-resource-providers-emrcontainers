package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.*;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.*;

public class CreateHandler extends BaseHandler<CallbackContext> {
    private Logger logger;
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonEMRContainers emrContainersClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        this.logger = logger;
        clientProxy = proxy;
        emrContainersClient = AmazonEMRContainersClientBuilder.defaultClient();

        final ResourceModel model = request.getDesiredResourceState();
        if (model.getArn() != null) {
            String readOnlyPropertyErrorMessage = "Arn is a ReadOnly property which cannot be specified in the template";
            logger.log(String.format("[ClientRequestToken: %s]\nResource failed in Create operation, Error: %s\n",
                    request.getClientRequestToken(), readOnlyPropertyErrorMessage));
            return ProgressEvent.defaultFailureHandler(
                    new Exception(readOnlyPropertyErrorMessage), HandlerErrorCode.InvalidRequest);
        }

        return ProgressEvent.defaultSuccessHandler(createVirtualCluster(model, request));
    }

    private ResourceModel createVirtualCluster(ResourceModel model, ResourceHandlerRequest<ResourceModel> request) {
        final CreateVirtualClusterRequest createVirtualClusterRequest = new CreateVirtualClusterRequest();
        createVirtualClusterRequest.setName(model.getName());
        createVirtualClusterRequest.setContainerProvider(Translator.translate(model.getContainerProvider()));
        createVirtualClusterRequest.setTags(request.getDesiredResourceTags());
        createVirtualClusterRequest.setClientToken(request.getClientRequestToken());

        try {
            final CreateVirtualClusterResult createVirtualClusterResult =
                    clientProxy.injectCredentialsAndInvoke(
                            createVirtualClusterRequest,
                            emrContainersClient::createVirtualCluster);

            return ResourceModel.builder()
                .arn(createVirtualClusterResult.getArn())
                .id(createVirtualClusterResult.getId())
                .containerProvider(model.getContainerProvider())
                .name(createVirtualClusterResult.getName())
                .tags(Translator.toTagSet(request.getDesiredResourceTags()))
                .build();
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("createVirtualCluster", e);
        } catch (AmazonEMRContainersException e) {
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }
    }
}

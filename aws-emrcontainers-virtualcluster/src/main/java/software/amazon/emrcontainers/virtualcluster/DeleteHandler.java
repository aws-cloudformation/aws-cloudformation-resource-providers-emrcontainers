package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.*;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonEMRContainers emrContainersClient;
    private ResourceStabilizer resourceStabilizer;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        clientProxy = proxy;
        emrContainersClient = AmazonEMRContainersClientBuilder.defaultClient();
        this.resourceStabilizer = ResourceStabilizer.builder()
                .emrContainersClient(emrContainersClient)
                .proxy(proxy)
                .model(model)
                .build();

        if (callbackContext != null && callbackContext.getIsDeleteInProgress()) {
            return resourceStabilizer.stabilizeResource(callbackContext);
        } else {
            return deleteCluster(model);
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteCluster(final ResourceModel model) {

        DeleteVirtualClusterRequest deleteVirtualClusterRequest =
                new DeleteVirtualClusterRequest();
        deleteVirtualClusterRequest.setId(model.getId());
        try {
            clientProxy.injectCredentialsAndInvoke(deleteVirtualClusterRequest, emrContainersClient::deleteVirtualCluster);
        } catch(ResourceNotFoundException e) {
            throw new CfnNotFoundException(e);
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("deleteVirtualCluster", e);
        } catch (AmazonEMRContainersException e) {
            if (e.getMessage().contains(Constants.VIRTUAL_CLUSTER_TERMINATED_MESSAGE)) {
                throw new CfnNotFoundException(e);
            }
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }

        CallbackContext stabilizationContext = CallbackContext.builder().isDeleteInProgress(true).build();
        return ProgressEvent.defaultInProgressHandler(stabilizationContext, Constants.CALLBACK_DELAY_SECONDS, model);
    }
}

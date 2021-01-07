package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.model.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;

@AllArgsConstructor
@Builder
public class ResourceStabilizer {
    private AmazonWebServicesClientProxy proxy;
    private AmazonEMRContainers emrContainersClient;
    private ResourceModel model;

    public ProgressEvent<ResourceModel, CallbackContext> stabilizeResource(CallbackContext callbackContext) {

        DescribeVirtualClusterRequest describeVirtualClusterRequest =
                new DescribeVirtualClusterRequest();
        describeVirtualClusterRequest.setId(model.getId());

        try {
            String state = proxy.injectCredentialsAndInvoke(
                    describeVirtualClusterRequest, emrContainersClient::describeVirtualCluster)
                    .getVirtualCluster().getState();
            if (VirtualClusterState.TERMINATED.toString().equals(state)) {
                return ProgressEvent.defaultSuccessHandler(null);
            } else if(VirtualClusterState.TERMINATING.toString().equals(state)) {
                return ProgressEvent.defaultInProgressHandler(callbackContext, Constants.CALLBACK_DELAY_SECONDS, model);
            } else {
                String errorMessage = String.format("Cluster %s failed to stabilize due to internal failure", model.getId());
                if (VirtualClusterState.ARRESTED.toString().equals(state)) {
                    errorMessage = String.format("Cluster %s is in arrested state and failed to stabilize", model.getId());
                }
                return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.NotStabilized, errorMessage);
            }
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("deleteVirtualCluster", e);
        } catch (AmazonEMRContainersException e) {
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }
    }
}

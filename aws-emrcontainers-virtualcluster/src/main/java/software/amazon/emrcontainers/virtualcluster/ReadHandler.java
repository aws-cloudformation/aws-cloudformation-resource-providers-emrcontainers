package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.*;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.*;

public class ReadHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonEMRContainers emrContainersClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        clientProxy = proxy;
        emrContainersClient = AmazonEMRContainersClientBuilder.defaultClient();

        return ProgressEvent.defaultSuccessHandler(describeVirtualCluster(model));
    }

    private ResourceModel describeVirtualCluster(final ResourceModel model) {

        final DescribeVirtualClusterRequest describeVirtualClusterRequest = new DescribeVirtualClusterRequest();
        describeVirtualClusterRequest.setId(model.getId());
        try {
            final VirtualCluster virtualCluster=
                clientProxy.injectCredentialsAndInvoke(
                    describeVirtualClusterRequest,
                    emrContainersClient::describeVirtualCluster).getVirtualCluster();

            if (virtualCluster.getState().equals(VirtualClusterState.TERMINATED.toString())) {
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getId());
            }

            return ResourceModel.builder()
                .arn(virtualCluster.getArn())
                .containerProvider(Translator.translate(virtualCluster.getContainerProvider()))
                .id(virtualCluster.getId())
                .name(virtualCluster.getName())
                .tags(Translator.toTagSet(virtualCluster.getTags()))
                .build();
        } catch(com.amazonaws.services.emrcontainers.model.ResourceNotFoundException e) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getId());
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("describeVirtualCluster", e);
        } catch (AmazonEMRContainersException e) {
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }
    }
}

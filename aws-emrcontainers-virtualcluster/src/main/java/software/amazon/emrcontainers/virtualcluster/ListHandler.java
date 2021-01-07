package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.ListVirtualClustersRequest;
import com.amazonaws.services.emrcontainers.model.VirtualCluster;
import com.amazonaws.services.emrcontainers.model.VirtualClusterState;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonEMRContainers emrContainersClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        emrContainersClient = AmazonEMRContainersClientBuilder.defaultClient();

        ListVirtualClustersRequest listVirtualClustersRequest = new ListVirtualClustersRequest();
        List<VirtualCluster> virtualClusterList = clientProxy.injectCredentialsAndInvoke(
                listVirtualClustersRequest, emrContainersClient::listVirtualClusters
        ).getVirtualClusters();

        final List<ResourceModel> models = virtualClusterList.stream()
            .filter((virtualCluster -> !virtualCluster.getState().equals(VirtualClusterState.TERMINATED.toString())))
            .map((virtualCluster) -> {
                ResourceModel model = ResourceModel.builder()
                    .arn(virtualCluster.getArn())
                    .containerProvider(Translator.translate(virtualCluster.getContainerProvider()))
                    .id(virtualCluster.getId())
                    .name(virtualCluster.getName())
                    .tags(Translator.toTagSet(virtualCluster.getTags()))
                    .build();
            return model;
        }).collect(Collectors.toList());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(models)
                .status(OperationStatus.SUCCESS)
                .build();
    }
}

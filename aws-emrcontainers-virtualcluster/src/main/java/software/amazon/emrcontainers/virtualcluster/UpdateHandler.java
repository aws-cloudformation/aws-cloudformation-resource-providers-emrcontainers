package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.*;
import com.amazonaws.util.StringUtils;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.*;
import java.util.stream.Collectors;

public class UpdateHandler extends BaseHandler<CallbackContext> {
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
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModel(update(model, request))
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private ResourceModel update(ResourceModel model, ResourceHandlerRequest<ResourceModel> request) {
        DescribeVirtualClusterRequest describeVirtualClusterRequest = new DescribeVirtualClusterRequest();
        describeVirtualClusterRequest.setId(model.getId());

        DescribeVirtualClusterResult describeVirtualClusterResult;
        VirtualCluster virtualCluster;
        try {
             describeVirtualClusterResult =
                clientProxy.injectCredentialsAndInvoke(describeVirtualClusterRequest, emrContainersClient::describeVirtualCluster);
            virtualCluster = describeVirtualClusterResult.getVirtualCluster();
            if (virtualCluster.getState().equals(VirtualClusterState.TERMINATED.toString())) {
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getId());
            }
        } catch(com.amazonaws.services.emrcontainers.model.ResourceNotFoundException e) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, model.getId());
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("describeVirtualCluster", e);
        } catch (AmazonEMRContainersException e) {
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }

        String arn = virtualCluster.getArn();
        final Set<Tag> existingTags = Translator.toTagSet(describeVirtualClusterResult.getVirtualCluster().getTags());
        final Set<Tag> latestTags = Translator.toTagSet(request.getDesiredResourceTags());

        final Set<Tag> tagsToRemove = new HashSet<>(existingTags);
        tagsToRemove.removeIf(latestTags::contains);

        final Set<Tag> tagsToAdd = new HashSet<>(latestTags);
        tagsToAdd.removeIf(existingTags::contains);

        if (tagsToRemove.size() > 0) {
            UntagResourceRequest untagResourceRequest = new UntagResourceRequest();
            untagResourceRequest.setResourceArn(arn);
            untagResourceRequest.setTagKeys(getFilteredTagKeys(tagsToRemove));
            clientProxy.injectCredentialsAndInvoke(untagResourceRequest, emrContainersClient::untagResource);
        }

        if (tagsToAdd.size() > 0) {
            TagResourceRequest tagResourceRequest = new TagResourceRequest();
            tagResourceRequest.setResourceArn(arn);
            tagResourceRequest.setTags(Translator.toTagMap(tagsToAdd));
            clientProxy.injectCredentialsAndInvoke(tagResourceRequest, emrContainersClient::tagResource);
        }

        model.setArn(arn);
        model.setName(virtualCluster.getName());
        return model;
    }

    /**
     * Get non-system tag keys for untag request.
     *
     * @param tags
     * @return
     */
    private List<String> getFilteredTagKeys(Set<Tag> tags) {
        return Translator.streamOfOrEmpty(tags)
                .filter(tag -> !StringUtils.beginsWithIgnoreCase(tag.getKey(), "aws:"))
                .map(Tag::getKey)
                .collect(Collectors.toList());
    }
}

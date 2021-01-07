package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.util.CollectionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-emrcontainers-virtualcluster.json");
    }

    /**
     * Override this method to support the use of both resource level tags and stack level tags. Resource level tags
     * are those defined within the template and stack level tags are those defined through cloudformation UI.
     *
     * @param resourceModel
     * @return
     */
    public Map<String, String> resourceDefinedTags(final ResourceModel resourceModel) {
        if(CollectionUtils.isNullOrEmpty(resourceModel.getTags()))
            return Collections.emptyMap();

        return resourceModel.getTags()
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }
}

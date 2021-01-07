package software.amazon.emrcontainers.virtualcluster;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Translator {

    static com.amazonaws.services.emrcontainers.model.ContainerProvider translate(ContainerProvider in) {

        com.amazonaws.services.emrcontainers.model.ContainerProvider containerProvider =
                new com.amazonaws.services.emrcontainers.model.ContainerProvider();
        containerProvider.setId(in.getId());
        containerProvider.setType(in.getType());
        containerProvider.setInfo(translate(in.getInfo()));

        return containerProvider;
    }

    static com.amazonaws.services.emrcontainers.model.ContainerInfo translate(ContainerInfo in) {

        com.amazonaws.services.emrcontainers.model.ContainerInfo containerInfo =
                new com.amazonaws.services.emrcontainers.model.ContainerInfo();
        containerInfo.setEksInfo(translate(in.getEksInfo()));

        return containerInfo;
    }

    static com.amazonaws.services.emrcontainers.model.EksInfo translate(EksInfo in) {

        com.amazonaws.services.emrcontainers.model.EksInfo eksInfo =
                new com.amazonaws.services.emrcontainers.model.EksInfo();
        eksInfo.setNamespace(in.getNamespace());

        return eksInfo;
    }

    static ContainerProvider translate(com.amazonaws.services.emrcontainers.model.ContainerProvider in) {
        ContainerProvider containerProvider = ContainerProvider.builder()
                .id(in.getId())
                .type(in.getType())
                .info(translate(in.getInfo()))
                .build();
        return containerProvider;
    }

    static ContainerInfo translate(com.amazonaws.services.emrcontainers.model.ContainerInfo in) {
        ContainerInfo containerInfo = ContainerInfo.builder()
                .eksInfo(translate(in.getEksInfo()))
                .build();
        return containerInfo;
    }

    static EksInfo translate(com.amazonaws.services.emrcontainers.model.EksInfo in) {
        EksInfo eksInfo = EksInfo.builder()
                .namespace(in.getNamespace())
                .build();
        return eksInfo;
    }

    static Set<Tag> toTagSet(Map<String, String> tags) {
        if (tags == null) {
            return Collections.emptySet();
        }
        return tags.keySet()
                .stream()
                .map((key) -> {
                    return Tag.builder().key(key).value(tags.get(key)).build();
                }).collect(Collectors.toSet());
    }

    static Map<String, String> toTagMap(Set<Tag> tags) {
        return streamOfOrEmpty(tags).collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }
}

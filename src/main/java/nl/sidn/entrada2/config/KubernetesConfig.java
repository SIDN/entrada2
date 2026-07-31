package nl.sidn.entrada2.config;

import org.springframework.cloud.kubernetes.commons.KubernetesNamespaceProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

/**
 * Workaround for https://github.com/spring-cloud/spring-cloud-kubernetes/issues/2270:
 * when both the fabric8 discovery and fabric8 config starters are on the classpath,
 * both {@code Fabric8AutoConfiguration} and {@code Fabric8BootstrapConfiguration}
 * register their own (non-{@code @ConditionalOnMissingBean}) {@link KubernetesNamespaceProvider}
 * bean, which causes an ambiguous bean error when it is autowired (e.g. by
 * {@code Fabric8InformerAutoConfiguration#selectiveNamespaces}). Defining our own
 * {@code @Primary} bean here resolves the ambiguity until the upstream fix
 * (spring-cloud-kubernetes 2025.1.3+) is released.
 */
@Configuration
public class KubernetesConfig {

  @Bean
  @Primary
  public KubernetesNamespaceProvider kubernetesNamespaceProvider(Environment environment) {
    return new KubernetesNamespaceProvider(environment);
  }

}

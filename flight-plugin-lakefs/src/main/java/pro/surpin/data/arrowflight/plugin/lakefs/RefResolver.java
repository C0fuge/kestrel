package pro.surpin.data.arrowflight.plugin.lakefs;

import java.io.IOException;
import java.util.Optional;

/**
 * Resolves a lakeFS branch to the commit id at its head.
 */
public interface RefResolver {

    /**
     * Looks up a branch.
     *
     * @param repository repository name
     * @param ref branch, tag or commit id
     * @return the head commit id when the ref is a branch, empty when it is not a branch (a
     *         commit id or tag, read as it is)
     * @throws IOException when lakeFS cannot be asked or refuses the request
     */
    Optional<String> branchHead(String repository, String ref) throws IOException;

    /**
     * Checks that a ref which is not a branch (a tag or a commit id) exists.
     *
     * @param repository repository name
     * @param ref tag or commit id
     * @return the commit id the ref names
     * @throws IOException when the repository or the ref does not exist, or lakeFS cannot be
     *         asked
     */
    default String refCommit(String repository, String ref) throws IOException {
        return ref;
    }
}

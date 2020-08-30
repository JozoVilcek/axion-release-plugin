package pl.allegro.tech.build.axion.release.infrastructure.git

import org.eclipse.jgit.api.*
import org.eclipse.jgit.api.errors.GitAPIException
import org.eclipse.jgit.api.errors.NoHeadException
import org.eclipse.jgit.errors.RepositoryNotFoundException
import org.eclipse.jgit.lib.*
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.revwalk.RevSort
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.transport.*
import org.eclipse.jgit.util.FS
import pl.allegro.tech.build.axion.release.domain.logging.ReleaseLogger
import pl.allegro.tech.build.axion.release.domain.scm.*

import java.util.regex.Pattern

class GitRepository implements ScmRepository {

    private static final ReleaseLogger logger = ReleaseLogger.Factory.logger(GitRepository)

    private static final String GIT_TAG_PREFIX = 'refs/tags/'

    private final TransportConfigFactory transportConfigFactory = new TransportConfigFactory()

    private final File repositoryDir

    private final Git jgitRepository

    private final ScmProperties properties

    GitRepository(ScmProperties properties) {
        try {
            this.repositoryDir = properties.directory
            RepositoryCache.FileKey key = RepositoryCache.FileKey.lenient(repositoryDir, FS.DETECTED);
            this.jgitRepository = Git.wrap(RepositoryCache.open(key, true));
            this.properties = properties
        }
        catch (RepositoryNotFoundException exception) {
            throw new ScmRepositoryUnavailableException(exception)
        }

        if (properties.attachRemote) {
            this.attachRemote(properties.remote, properties.remoteUrl)
        }
        if (properties.fetchTags) {
            this.fetchTags(properties.identity, properties.remote)
        }
    }

    @Override
    public String id() {
        return repositoryDir.getAbsolutePath();
    }

    /**
     * This fetch method behaves like git fetch, meaning it only fetches thing without merging.
     * As a result, any fetched tags will not be visible via GitRepository tag listing methods
     * because they do commit-tree walk, not tag listing.
     *
     * This method is only useful if you have bare repo on CI systems, where merge is not neccessary, because newest
     * version of content has already been fetched.
     */
    @Override
    void fetchTags(ScmIdentity identity, String remoteName) {
        FetchCommand fetch = jgitRepository.fetch()
            .setRemote(remoteName)
            .setTagOpt(TagOpt.FETCH_TAGS)
            .setTransportConfigCallback(transportConfigFactory.create(identity))
        fetch.call()
    }

    @Override
    void tag(String tagName) {
        String headId = head().name()

        boolean isOnExistingTag = jgitRepository.tagList().call().any({
            it -> it.name == GIT_TAG_PREFIX + tagName && jgitRepository.repository.peel(it).peeledObjectId.name == headId
        })
        if (!isOnExistingTag) {
            jgitRepository.tag()
                .setName(tagName)
                .call()
            ScmCache.getInstance().invalidate(this)
        } else {
            logger.debug("The head commit $headId already has the tag $tagName.")
        }
    }

    private ObjectId head() {
        return jgitRepository.repository.resolve(Constants.HEAD)
    }

    @Override
    void dropTag(String tagName) {
        try {
            jgitRepository.tagDelete()
                .setTags(GIT_TAG_PREFIX + tagName)
                .call()
            ScmCache.getInstance().invalidate(this)
        } catch (GitAPIException e) {
            throw new ScmException(e)
        }
    }

    @Override
    ScmPushResult push(ScmIdentity identity, ScmPushOptions pushOptions) {
        return push(identity, pushOptions, false)
    }

    ScmPushResult push(ScmIdentity identity, ScmPushOptions pushOptions, boolean all) {
        PushCommand command = pushCommand(identity, pushOptions.remote, all)

        // command has to be called twice:
        // once for commits (only if needed)
        if (!pushOptions.pushTagsOnly) {
            ScmPushResult result = verifyPushResults(callPush(command))
            if (!result.success) {
                return result
            }
        }

        // and another time for tags
        return verifyPushResults(callPush(command.setPushTags()))
    }

    private Iterable<PushResult> callPush(PushCommand pushCommand) {
        try {
            def result = pushCommand.call()
            ScmCache.getInstance().invalidate(this);
            return result
        } catch (GitAPIException e) {
            throw new ScmException(e)
        }
    }

    private ScmPushResult verifyPushResults(Iterable<PushResult> pushResults) {
        PushResult pushResult = pushResults.iterator().next()
        Iterator<RemoteRefUpdate> remoteUpdates = pushResult.getRemoteUpdates().iterator()

        RemoteRefUpdate failedRefUpdate = remoteUpdates.find({
            it.getStatus() != RemoteRefUpdate.Status.OK && it.getStatus() != RemoteRefUpdate.Status.UP_TO_DATE
        })

        return new ScmPushResult(failedRefUpdate == null, Optional.ofNullable(pushResult.messages))
    }

    private PushCommand pushCommand(ScmIdentity identity, String remoteName, boolean all) {
        PushCommand push = jgitRepository.push()
        push.remote = remoteName

        if (all) {
            push.setPushAll()
        }
        push.transportConfigCallback = transportConfigFactory.create(identity)

        return push
    }

    @Override
    void attachRemote(String remoteName, String remoteUrl) {
        Config config = jgitRepository.repository.config

        RemoteConfig remote = new RemoteConfig(config, remoteName)
        // clear other push specs
        List<URIish> pushUris = new ArrayList<>(remote.pushURIs)
        for (URIish uri : pushUris) {
            remote.removePushURI(uri)
        }

        remote.addPushURI(new URIish(remoteUrl))
        remote.update(config)

        config.save()
    }

    @Override
    void commit(List patterns, String message) {
        if (!patterns.isEmpty()) {
            String canonicalPath = Pattern.quote(repositoryDir.canonicalPath + File.separatorChar)
            AddCommand command = jgitRepository.add()
            patterns.collect({ it.replaceFirst(canonicalPath, '') }).each({ command.addFilepattern(it) })
            command.call()
        }
        jgitRepository.commit()
            .setMessage(message)
            .call()
        ScmCache.getInstance().invalidate(this)
    }

    ScmPosition currentPosition() {
        String revision = ''
        String shortRevision = ''
        if (hasCommits()) {
            ObjectId head = head()
            revision = head.name()
            shortRevision = revision[0..(7 - 1)]
        }

        return new ScmPosition(
            revision,
            shortRevision,
            branchName()
        )
    }

    /**
     * @return branch name or 'HEAD' when in detached state, unless it is overridden by 'overriddenBranchName'
     */
    private String branchName() {
        String currentBranch = Repository.shortenRefName(jgitRepository.repository.exactRef(Constants.HEAD)?.target.name)
        if (currentBranch == 'HEAD' && properties.overriddenBranchName != null) {
            return Repository.shortenRefName(properties.overriddenBranchName)
        }
        return currentBranch
    }

    @Override
    TagsOnCommit latestTags(Pattern pattern) {
        return latestTagsInternal(pattern, null, true)
    }

    @Override
    TagsOnCommit latestTags(Pattern pattern, String sinceCommit) {
        return latestTagsInternal(pattern, sinceCommit, false)
    }

    private TagsOnCommit latestTagsInternal(Pattern pattern, String maybeSinceCommit, boolean inclusive) {
        List<TagsOnCommit> taggedCommits = taggedCommitsInternal(pattern, maybeSinceCommit, inclusive, true)
        return taggedCommits.isEmpty() ? TagsOnCommit.empty() : taggedCommits[0]
    }

    @Override
    List<TagsOnCommit> taggedCommits(Pattern pattern) {
        return taggedCommitsInternal(pattern, null, true, false)
    }

    private List<TagsOnCommit> taggedCommitsInternal(Pattern pattern,
                                                     String maybeSinceCommit,
                                                     boolean inclusive,
                                                     boolean stopOnFirstTag) {
        List<TagsOnCommit> taggedCommits = new ArrayList<>()
        if (!hasCommits()) {
            return taggedCommits
        }

        ObjectId headId = jgitRepository.repository.resolve(Constants.HEAD)

        ObjectId startingCommit
        if (maybeSinceCommit != null) {
            startingCommit = ObjectId.fromString(maybeSinceCommit)
        } else {
            startingCommit = headId
        }

        RevWalk walk = walker(startingCommit)
        try {
            if (!inclusive) {
                walk.next();
            }
            Map<String, List<String>> allTags = tagsMatching(pattern, walk)
            if (stopOnFirstTag) {
                // stopOnFirstTag needs to get latest tag, therefore the order does matter
                // order is given by walking the repository commits. this can be slower in some
                // situations than returning all tagged commits
                RevCommit currentCommit;
                for (currentCommit = walk.next(); currentCommit != null; currentCommit = walk.next()) {
                    List<String> tagList = allTags.get(currentCommit.getId().getName());
                    if (tagList != null) {
                        TagsOnCommit taggedCommit = new TagsOnCommit(currentCommit.getId().name(), tagList,
                            headId.name().equals(currentCommit.getId().name()))
                        taggedCommits.add(taggedCommit);
                        break;
                    }
                }
            } else {
                // order not needed, we can just return all tagged commits
                allTags.each {it -> taggedCommits
                    .add(new TagsOnCommit(it.getKey(), it.getValue(), headId.name().equals(it.getKey())))}
            }
        } finally {
            walk.dispose();
        }

        return taggedCommits
    }

    private RevWalk walker(ObjectId startingCommit) {
        RevWalk walk = new RevWalk(jgitRepository.repository)

        // explicitly set to NONE
        // TOPO sorting forces all commits in repo to be read in memory,
        // making walk incredibly slow
        walk.sort(RevSort.NONE)
        RevCommit head = walk.parseCommit(startingCommit)
        walk.markStart(head)
        return walk
    }

    private Map<String, List<String>> tagsMatching(Pattern pattern, RevWalk walk) {
        List<Tuple2<Ref, RevCommit>> parsedTagList = ScmCache.getInstance().parsedTagList(this, jgitRepository, walk);

        Map<String, List<String>> tags = new HashMap<>()
        parsedTagList
            .collect {pair -> new TagNameAndId(
                pair.getFirst().getName().substring(GIT_TAG_PREFIX.length()),
                pair.getSecond().getName())
            }.findAll { t -> pattern.matcher(t.name).matches() }
            .each {
                def list = tags.get(it.id)
                if (list == null) {
                    list = new ArrayList<String>()
                    tags.put(it.id, list)
                }
                list.add(it.name)
            }

        return tags
//        return parsedTagList.stream()
//            .map(pair -> new TagNameAndId(
//                pair.getFirst().getName().substring(GIT_TAG_PREFIX.length()),
//                pair.getSecond().getName()))
//            .filter(t -> pattern.matcher(t.name).matches())
//            .collect(
//                HashMap::new,
//                (m, t) -> m.computeIfAbsent(t.id, (s) -> new ArrayList<>()).add(t.name),
//                HashMap::putAll);
    }

    private final static class TagNameAndId {
        final String name;
        final String id;

        TagNameAndId(String name, String id) {
            this.name = name;
            this.id = id;
        }
    }

    private boolean hasCommits() {
        LogCommand log = jgitRepository.log()
        log.maxCount = 1

        try {
            log.call()
            return true
        }
        catch (NoHeadException exception) {
            return false
        }
    }

    @Override
    boolean remoteAttached(String remoteName) {
        Config config = jgitRepository.repository.config
        return config.getSubsections('remote').any { it == remoteName }
    }

    @Override
    boolean checkUncommittedChanges() {
        return !jgitRepository.status().call().isClean()
    }

    @Override
    boolean checkAheadOfRemote() {
        String branchName = jgitRepository.repository.fullBranch
        BranchTrackingStatus status = BranchTrackingStatus.of(jgitRepository.repository, branchName)

        if (status == null) {
            throw new ScmException("Branch $branchName is not set to track another branch")
        }

        return status.aheadCount != 0 || status.behindCount != 0
    }

    Status listChanges() {
        return jgitRepository.status().call()
    }

    @Override
    List<String> lastLogMessages(int messageCount) {
        return jgitRepository.log()
            .setMaxCount(messageCount)
            .call()*.fullMessage
    }
}

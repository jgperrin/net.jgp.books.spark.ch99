package net.jgp.books.spark.ch99.covid19.x.utils;

import java.io.File;
import java.io.IOException;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GitUtils {
  private static Logger log =
      LoggerFactory.getLogger(GitUtils.class);

  /**
   * Syncs (commits or pulls) a repo.
   * 
   * @param repositoryUrl
   * @param destinationPath
   * @return
   */
  public static boolean syncRepository(String repositoryUrl,
      String destinationPath) {
    log.debug("-> syncRepository()");

    Git git;
    File dest = new File(destinationPath);

    if (dest.exists()) {
      // pull
      Repository localRepo;
      try {
        localRepo = new FileRepository(destinationPath + "/.git");
      } catch (IOException e) {
        log.error("Exception reading a Git repo at {}, got {}.",
            destinationPath, e.getMessage());
        return false;
      }
      git = new Git(localRepo);
      PullCommand pullCmd = git.pull();
      try {
        pullCmd.call();
      } catch (GitAPIException e) {
        e.printStackTrace();
        return false;
      }
    } else {
      // clone
      try {
        git = Git.cloneRepository()
            .setURI(repositoryUrl)
            .setDirectory(dest)
            .call();
      } catch (GitAPIException e) {
        log.error(
            "Exception while creating a local Git repo at {}, got {}.",
            repositoryUrl, e.getMessage());
        return false;
      }
    }

    log.debug("<- syncRepository() - ok");
    return true;
  }

}

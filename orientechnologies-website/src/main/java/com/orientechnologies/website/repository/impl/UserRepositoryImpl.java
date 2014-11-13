package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.User;
import com.orientechnologies.website.repository.UserRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 20/10/14.
 */

@Repository
public class UserRepositoryImpl extends OrientBaseRepository<User> implements UserRepository {

  @Override
  public User findUserByLogin(String login) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where name = '%s'", getEntityClass().getSimpleName(), login);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public User findUserOrCreateByLogin(String login, Long id) {

    User user = findUserByLogin(login);
    if (user == null) {
      user = new User(login, null, null);
      user.setId(id);
      user = save(user);
    }
    return user;
  }

  @Override
  public User findByGithubToken(String token) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where token = '%s'", getEntityClass().getSimpleName(), token);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Class<User> getEntityClass() {
    return User.class;
  }

  @Override
  public OTypeHolder<User> getHolder() {
    return com.orientechnologies.website.model.schema.OUser.EMAIL;
  }
}

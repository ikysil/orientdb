/* Generated By:JJTree: Do not edit this line. OAlterClassStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OAlterClassStatement extends ODDLStatement {

  /** the name of the class */
  protected OIdentifier name;

  /** the class property to be altered */
  public OClass.ATTRIBUTES property;

  protected OIdentifier identifierValue;
  protected List<OIdentifier> identifierListValue;
  protected Boolean add;
  protected Boolean remove;
  protected ONumber numberValue;
  protected Boolean booleanValue;
  public OIdentifier customKey;
  public OExpression customValue;

  protected OInteger defaultClusterId;
  protected OIdentifier defaultClusterName;

  // only to manage 'round-robin' as a cluster selection strategy (not a valid identifier)
  protected String customString;

  protected boolean unsafe;

  public OAlterClassStatement(int id) {
    super(id);
  }

  public OAlterClassStatement(OrientSql p, int id) {
    super(p, id);
  }

  public void addIdentifierListValue(OIdentifier id) {
    if (this.identifierListValue == null) {
      this.identifierListValue = new ArrayList<>();
    }
    this.identifierListValue.add(id);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("ALTER CLASS ");
    name.toString(params, builder);
    if (property != null) {
      builder.append(" " + property.name() + " ");
      switch (property) {
        case NAME:
        case SHORTNAME:
        case ADDCLUSTER:
        case REMOVECLUSTER:
        case DESCRIPTION:
        case ENCRYPTION:
          if (numberValue != null) {
            numberValue.toString(params, builder); // clusters only
          } else if (identifierValue != null) {
            identifierValue.toString(params, builder);
          } else {
            builder.append("null");
          }
          break;
        case CLUSTERSELECTION:
          if (identifierValue != null) {
            identifierValue.toString(params, builder);
          } else if (customString != null) {
            builder.append('\'').append(customString).append('\'');
          } else {
            builder.append("null");
          }
          break;
        case SUPERCLASS:
          if (Boolean.TRUE.equals(add)) {
            builder.append("+");
          } else if (Boolean.TRUE.equals(remove)) {
            builder.append("-");
          }
          if (identifierValue == null) {
            builder.append("null");
          } else {
            identifierValue.toString(params, builder);
          }
          break;
        case SUPERCLASSES:
          if (identifierListValue == null) {
            builder.append("null");
          } else {
            boolean first = true;
            for (OIdentifier ident : identifierListValue) {
              if (!first) {
                builder.append(", ");
              }
              ident.toString(params, builder);
              first = false;
            }
          }
          break;
        case OVERSIZE:
          numberValue.toString(params, builder);
          break;
        case STRICTMODE:
        case ABSTRACT:
          builder.append(booleanValue.booleanValue());
          break;
        case CUSTOM:
          customKey.toString(params, builder);
          builder.append("=");
          if (customValue == null) {
            builder.append("null");
          } else {
            customValue.toString(params, builder);
          }
          break;
      }
    } else if (defaultClusterId != null) {
      builder.append(" DEFAULTCLUSTER ");
      defaultClusterId.toString(params, builder);
    } else if (defaultClusterName != null) {
      builder.append(" DEFAULTCLUSTER ");
      defaultClusterName.toString(params, builder);
    }
    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("ALTER CLASS ");
    name.toGenericStatement(builder);
    if (property != null) {
      builder.append(" " + property.name() + " ");
      switch (property) {
        case NAME:
        case SHORTNAME:
        case ADDCLUSTER:
        case REMOVECLUSTER:
        case DESCRIPTION:
        case ENCRYPTION:
          if (numberValue != null) {
            numberValue.toGenericStatement(builder); // clusters only
          } else if (identifierValue != null) {
            identifierValue.toGenericStatement(builder);
          } else {
            builder.append(PARAMETER_PLACEHOLDER);
          }
          break;
        case CLUSTERSELECTION:
          if (identifierValue != null) {
            identifierValue.toGenericStatement(builder);
          } else {
            builder.append(PARAMETER_PLACEHOLDER);
          }
          break;
        case SUPERCLASS:
          if (Boolean.TRUE.equals(add)) {
            builder.append("+");
          } else if (Boolean.TRUE.equals(remove)) {
            builder.append("-");
          }
          if (identifierValue == null) {
            builder.append(PARAMETER_PLACEHOLDER);
          } else {
            identifierValue.toGenericStatement(builder);
          }
          break;
        case SUPERCLASSES:
          if (identifierListValue == null) {
            builder.append(PARAMETER_PLACEHOLDER);
          } else {
            boolean first = true;
            for (OIdentifier ident : identifierListValue) {
              if (!first) {
                builder.append(", ");
              }
              ident.toGenericStatement(builder);
              first = false;
            }
          }
          break;
        case OVERSIZE:
          numberValue.toGenericStatement(builder);
          break;
        case STRICTMODE:
        case ABSTRACT:
          builder.append(booleanValue.booleanValue());
          break;
        case CUSTOM:
          customKey.toGenericStatement(builder);
          builder.append("=");
          if (customValue == null) {
            builder.append("null");
          } else {
            customValue.toGenericStatement(builder);
          }
          break;
      }
    } else if (defaultClusterId != null) {
      builder.append(" DEFAULTCLUSTER ");
      defaultClusterId.toGenericStatement(builder);
    } else if (defaultClusterName != null) {
      builder.append(" DEFAULTCLUSTER ");
      defaultClusterName.toGenericStatement(builder);
    }
    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  public OStatement copy() {
    OAlterClassStatement result = new OAlterClassStatement(-1);
    result.name = name == null ? null : name.copy();
    result.property = property;
    result.identifierValue = identifierValue == null ? null : identifierValue.copy();
    result.identifierListValue =
        identifierListValue == null
            ? null
            : identifierListValue.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.add = add;
    result.remove = remove;
    result.numberValue = numberValue == null ? null : numberValue.copy();
    result.booleanValue = booleanValue;
    result.customKey = customKey == null ? null : customKey.copy();
    result.customValue = customValue == null ? null : customValue.copy();
    result.customString = customString;
    result.defaultClusterId = defaultClusterId == null ? null : defaultClusterId.copy();
    result.defaultClusterName = defaultClusterName == null ? null : defaultClusterName.copy();
    result.unsafe = unsafe;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OAlterClassStatement that = (OAlterClassStatement) o;

    if (unsafe != that.unsafe) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (property != that.property) return false;
    if (identifierValue != null
        ? !identifierValue.equals(that.identifierValue)
        : that.identifierValue != null) return false;
    if (identifierListValue != null
        ? !identifierListValue.equals(that.identifierListValue)
        : that.identifierListValue != null) return false;
    if (add != null ? !add.equals(that.add) : that.add != null) return false;
    if (remove != null ? !remove.equals(that.remove) : that.remove != null) return false;
    if (numberValue != null ? !numberValue.equals(that.numberValue) : that.numberValue != null)
      return false;
    if (booleanValue != null ? !booleanValue.equals(that.booleanValue) : that.booleanValue != null)
      return false;
    if (customKey != null ? !customKey.equals(that.customKey) : that.customKey != null)
      return false;
    if (customValue != null ? !customValue.equals(that.customValue) : that.customValue != null)
      return false;
    if (defaultClusterId != null
        ? !defaultClusterId.equals(that.defaultClusterId)
        : that.defaultClusterId != null) return false;
    if (defaultClusterName != null
        ? !defaultClusterName.equals(that.defaultClusterName)
        : that.defaultClusterName != null) return false;
    return customString != null
        ? customString.equals(that.customString)
        : that.customString == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (property != null ? property.hashCode() : 0);
    result = 31 * result + (identifierValue != null ? identifierValue.hashCode() : 0);
    result = 31 * result + (identifierListValue != null ? identifierListValue.hashCode() : 0);
    result = 31 * result + (add != null ? add.hashCode() : 0);
    result = 31 * result + (remove != null ? remove.hashCode() : 0);
    result = 31 * result + (numberValue != null ? numberValue.hashCode() : 0);
    result = 31 * result + (booleanValue != null ? booleanValue.hashCode() : 0);
    result = 31 * result + (customKey != null ? customKey.hashCode() : 0);
    result = 31 * result + (customValue != null ? customValue.hashCode() : 0);
    result = 31 * result + (defaultClusterId != null ? defaultClusterId.hashCode() : 0);
    result = 31 * result + (defaultClusterName != null ? defaultClusterName.hashCode() : 0);
    result = 31 * result + (customString != null ? customString.hashCode() : 0);
    result = 31 * result + (unsafe ? 1 : 0);
    return result;
  }

  @Override
  public OExecutionStream executeDDL(OCommandContext ctx) {
    OClass oClass = ctx.getDatabase().getMetadata().getSchema().getClass(name.getStringValue());
    if (oClass == null) {
      throw new OCommandExecutionException("Class not found: " + name);
    }
    if (property != null) {
      switch (property) {
        case NAME:
          if (!unsafe) {
            checkNotEdge(oClass);
            checkNotIndexed(oClass);
          }
          try {
            oClass.setName(identifierValue.getStringValue());
          } catch (Exception e) {
            OException x =
                OException.wrapException(
                    new OCommandExecutionException("Invalid class name: " + toString()), e);
            throw x;
          }
          break;
        case SHORTNAME:
          if (identifierValue != null) {
            try {
              oClass.setShortName(identifierValue.getStringValue());
            } catch (Exception e) {
              OException x =
                  OException.wrapException(
                      new OCommandExecutionException("Invalid class name: " + toString()), e);
              throw x;
            }
          } else {
            throw new OCommandExecutionException("Invalid class name: " + toString());
          }
          break;
        case ADDCLUSTER:
          if (identifierValue != null) {
            oClass.addCluster(identifierValue.getStringValue());
          } else if (numberValue != null) {
            oClass.addClusterId(numberValue.getValue().intValue());
          } else {
            throw new OCommandExecutionException("Invalid cluster value: " + toString());
          }
          break;
        case REMOVECLUSTER:
          int clusterId = -1;
          if (identifierValue != null) {
            clusterId = ctx.getDatabase().getClusterIdByName(identifierValue.getStringValue());
            if (clusterId < 0) {
              throw new OCommandExecutionException("Cluster not found: " + toString());
            }
          } else if (numberValue != null) {
            clusterId = numberValue.getValue().intValue();
          } else {
            throw new OCommandExecutionException("Invalid cluster value: " + toString());
          }
          oClass.removeClusterId(clusterId);
          break;
        case DESCRIPTION:
          if (identifierValue != null) {
            oClass.setDescription(identifierValue.getStringValue());
          } else {
            throw new OCommandExecutionException("Invalid class name: " + toString());
          }
          break;
        case ENCRYPTION:
          // TODO

          break;
        case CLUSTERSELECTION:
          if (identifierValue != null) {
            oClass.setClusterSelection(identifierValue.getStringValue());
          } else if (customString != null) {
            oClass.setClusterSelection(customString);
          } else {
            oClass.setClusterSelection("null");
          }
          break;
        case SUPERCLASS:
          doSetSuperclass(ctx, oClass, identifierValue);
          break;
        case SUPERCLASSES:
          if (identifierListValue == null) {
            oClass.setSuperClasses(Collections.EMPTY_LIST);
          } else {
            doSetSuperclasses(ctx, oClass, identifierListValue);
          }
          break;
        case OVERSIZE:
          oClass.setOverSize(numberValue.getValue().floatValue());
          break;
        case STRICTMODE:
          oClass.setStrictMode(booleanValue.booleanValue());
          break;
        case ABSTRACT:
          oClass.setAbstract(booleanValue.booleanValue());
          break;
        case CUSTOM:
          Object value = null;
          if (customValue != null) {
            value = customValue.execute((OResult) null, ctx);
          }
          if (value != null) {
            value = "" + value;
          }
          oClass.setCustom(customKey.getStringValue(), (String) value);
          break;
      }
    } else if (defaultClusterId != null) {
      oClass.setDefaultClusterId(defaultClusterId.getValue().intValue());
    } else if (defaultClusterName != null) {
      int clusterId = ctx.getDatabase().getClusterIdByName(defaultClusterName.getStringValue());
      oClass.setDefaultClusterId(clusterId);
    }
    OResultInternal result = new OResultInternal();
    result.setProperty("operation", "ALTER CLASS");
    result.setProperty("className", name.getStringValue());
    result.setProperty("result", "OK");
    return OExecutionStream.singleton(result);
  }

  private void checkNotIndexed(OClass oClass) {
    Set<OIndex> indexes = oClass.getIndexes();
    if (indexes != null && indexes.size() > 0) {
      throw new OCommandExecutionException(
          "Cannot rename class '"
              + oClass.getName()
              + "' because it has indexes defined on it. Drop indexes before or use UNSAFE (at your"
              + " won risk)");
    }
  }

  private void checkNotEdge(OClass oClass) {
    if (oClass.isSubClassOf("E")) {
      throw new OCommandExecutionException(
          "Cannot alter class '"
              + oClass
              + "' because is an Edge class and could break vertices. Use UNSAFE if you want to"
              + " force it");
    }
  }

  private void doSetSuperclass(OCommandContext ctx, OClass oClass, OIdentifier superclassName) {
    if (superclassName == null) {
      throw new OCommandExecutionException("Invalid superclass name: " + toString());
    }
    OClass superclass =
        ctx.getDatabase().getMetadata().getSchema().getClass(superclassName.getStringValue());
    if (superclass == null) {
      throw new OCommandExecutionException("superclass not found: " + toString());
    }
    if (Boolean.TRUE.equals(add)) {
      oClass.addSuperClass(superclass);
    } else if (Boolean.TRUE.equals(remove)) {
      oClass.removeSuperClass(superclass);
    } else {
      oClass.setSuperClasses(Collections.singletonList(superclass));
    }
  }

  private void doSetSuperclasses(
      OCommandContext ctx, OClass oClass, List<OIdentifier> superclassNames) {
    if (superclassNames == null) {
      throw new OCommandExecutionException("Invalid superclass name: " + toString());
    }
    List<OClass> superclasses = new ArrayList<>();
    for (OIdentifier superclassName : superclassNames) {
      OClass superclass =
          ctx.getDatabase().getMetadata().getSchema().getClass(superclassName.getStringValue());
      if (superclass == null) {
        throw new OCommandExecutionException("superclass not found: " + toString());
      }
      superclasses.add(superclass);
    }
    if (Boolean.TRUE.equals(add)) {
      for (OClass superclass : superclasses) {
        oClass.addSuperClass(superclass);
      }
    } else if (Boolean.TRUE.equals(remove)) {
      for (OClass superclass : superclasses) {
        oClass.removeSuperClass(superclass);
      }
    } else {
      oClass.setSuperClasses(superclasses);
    }
  }
}
/* JavaCC - OriginalChecksum=4668bb1cd336844052df941f39bdb634 (do not edit this line) */

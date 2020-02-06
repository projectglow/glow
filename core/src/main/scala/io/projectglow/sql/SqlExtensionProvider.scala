/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.sql

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{SQLUtils, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.yaml.snakeyaml.Yaml

import io.projectglow.common.WithUtils
import io.projectglow.sql.expressions._
import io.projectglow.sql.optimizer.{ReplaceExpressionsRule, ResolveAggregateFunctionsRule}

// TODO(hhd): Spark 3.0 allows extensions to register functions. After Spark 3.0 is released,
// we should move all extensions into this class.
class GlowSQLExtensions extends (SparkSessionExtensions => Unit) {
  val resolutionRules: Seq[Rule[LogicalPlan]] =
    Seq(ReplaceExpressionsRule, ResolveAggregateFunctionsRule)
  val optimizations: Seq[Rule[LogicalPlan]] = Seq()

  def apply(extensions: SparkSessionExtensions): Unit = {
    resolutionRules.foreach { r =>
      extensions.injectResolutionRule(_ => r)
    }
    optimizations.foreach(r => extensions.injectOptimizerRule(_ => r))
  }
}

object SqlExtensionProvider {
  private val FUNCTION_YAML_PATH = "functions.yml"

  private def loadFunctionDefinitions(resourcePath: String): Iterable[JMap[String, Any]] = {
    val yml = new Yaml()
    WithUtils.withCloseable(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath)) { stream =>
      val groups = yml.loadAs(stream, classOf[JMap[String, JMap[String, Any]]])
      groups
        .values()
        .asScala
        .flatMap(group => group.asScala("functions").asInstanceOf[JList[JMap[String, Any]]].asScala)
    }
  }

  private def parameterError(functionName: String, params: Int): Exception = {
    SQLUtils.newAnalysisException(
      s"Invalid number of parameters for function '$functionName': $params")
  }

  /**
   * Register SQL functions based on a yaml function definition file.
   */
  def registerFunctions(
      conf: SQLConf,
      functionRegistry: FunctionRegistry,
      resourcePath: String = FUNCTION_YAML_PATH): Unit = {

    loadFunctionDefinitions(resourcePath).foreach { _function =>
      val function = _function.asScala
      val id = FunctionIdentifier(function("name").asInstanceOf[String])
      //      println(s"Building $id")
      val exprClass = function("expr_class").asInstanceOf[String]
      val args = function("args").asInstanceOf[JList[JMap[String, Any]]].asScala
      val argsDoc = args.map { _arg =>
        val arg = _arg.asScala
        val suffix = if (arg.get("is_optional").exists(_.asInstanceOf[Boolean])) {
          " (optional)"
        } else if (arg.get("is_var_args").exists(_.asInstanceOf[Boolean])) {
          " (repeated)"
        } else {
          ""
        }
        s"${arg("name")}: ${arg("doc")} $suffix"
      }.mkString("\n")
      val info = new ExpressionInfo(
        exprClass,
        null,
        function("name").asInstanceOf[String],
        function("doc").asInstanceOf[String],
        argsDoc,
        "",
        "",
        function("since").asInstanceOf[String]
      )
      functionRegistry.registerFunction(
        id,
        info,
        exprs => {
          val clazz = Class.forName(exprClass, true, Thread.currentThread().getContextClassLoader)
          val children = args.zipWithIndex.flatMap {
            case (_arg: JMap[String, Any], idx: Int) =>
              val arg = _arg.asScala
              if (arg.get("is_optional").exists(_.asInstanceOf[Boolean]) && idx >= exprs.size) {
                None
              } else if (arg.get("is_var_args").exists(_.asInstanceOf[Boolean])) {
                Some(exprs.slice(idx, exprs.size))
              } else if (idx >= exprs.size) {
                throw parameterError(id.funcName, exprs.size)
              } else if (idx == args.size - 1 && exprs.size != args.size) {
                throw parameterError(id.funcName, exprs.size)
              } else {
                Some(exprs(idx))
              }
          }
          val constructor = clazz
            .getConstructors
            .find(_.getParameterCount == children.size)
            .getOrElse(throw parameterError(id.funcName, exprs.size))

          ExpressionHelper.rewrite(constructor.newInstance(children: _*).asInstanceOf[Expression])
        }
      )
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by HadoopFSDelegationTokenProviderapplicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.security

// scalastyle:off

import java.net.URI
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.internal.config._
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkException}


private[deploy] class HadoopMultiFSDelegationTokenProvider()
    extends HadoopDelegationTokenProvider with Logging {

  private var tokenRenewalInterval: Option[Long] = null

  override val serviceName: String = "hadoopmfs"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {

//    val fsToGetTokens = fileSystems(hadoopConf)

    val url = sparkConf.get(MULTI_KRB_HDFS_URL).orNull
    val principal2 = sparkConf.get(MULTI_KRB_PRINCIPAL).orNull
    val keytab2 = sparkConf.get(MULTI_KRB_KEYTAB).orNull

//    val principal2 = "test_p@TPL-HADOOP-WH.COM"
//    val keytab2 = "/home/jinhanzhong/test/opt/keytab/test_p.keytab"
    logInfo(s"login $principal2")
    val kerberosUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal2, keytab2);
//    val tokenRenewer = getTokenRenewer(hadoopConf)
    // must set to empty string to skip renew process in resource manager
    // org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer#handleAppSubmitEvent
    val tokenRenewer = ""

    kerberosUGI.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val conf = new Configuration()
        val targetPath = new URI(url)
        val fs = FileSystem.get(targetPath,conf)
        //      fs.getFileStatus(filePath); // ← kerberosUGI can authenticate via Kerberos
        // get delegation tokens, add them to the provided credentials. set renewer to ‘yarn’
        logInfo(s"add delegation token from $url with tokenRenewer $tokenRenewer")
        fs.addDelegationTokens(tokenRenewer, creds);
      }
    })

    //// Get the token renewal interval if it is not set. It will only be called once.
    //if (tokenRenewalInterval == null) {
    //  tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, sparkConf, fsToGetTokens)
    //}
    None
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    sparkConf.get(MULTI_KRB_PRINCIPAL).orNull != null
  }

  private def getTokenRenewer(hadoopConf: Configuration): String = {
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    logDebug("Delegation token renewer is: " + tokenRenewer)

    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      logError(errorMessage)
      throw new

          SparkException(errorMessage)
    }
    tokenRenewer
  }

  /*
  private def getTokenRenewalInterval(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      filesystems: Set[FileSystem]): Option[Long] = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    sparkConf.get(PRINCIPAL).flatMap { renewer =>
      val creds = new Credentials()
      fetchDelegationTokens(renewer, filesystems, creds)

      val renewIntervals = creds.getAllTokens.asScala.filter {
        _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
      }.flatMap { token =>
        Try {
          val newExpiration = token.renew(hadoopConf)
          val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          val interval = newExpiration - identifier.getIssueDate
          logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
          interval
        }.toOption
      }
      if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
    }
  }
  */
}

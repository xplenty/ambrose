/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.ambrose.pig;

import com.twitter.ambrose.service.impl.HttpStatsWriteService;


/**
 * Sublclass of AmbrosePigProgressNotificationListener that starts a ScriptStatusServer embedded in
 * the running Pig client VM. Stats are collected using by this class via InMemoryStatsService,
 * which is what serves stats to ScriptStatusServer.
 * <P>
 * To use this class with pig, start pig as follows:
 * <pre>
 * $ bin/pig \
 *  -Dpig.notification.listener=com.twitter.ambrose.pig.EmbeddedAmbrosePigProgressNotificationListener \
 *  -f path/to/script.pig
 * </pre>
 * Additional <pre>-D</pre> options can be set as system as system properties. Note that these must
 * be set via <pre>PIG_OPTS</pre>. For example, <pre>export PIG_OPTS=-Dambrose.port.number=8188</pre>.
 * <ul>
 *   <li><pre>ambrose.port.number</pre> (default=8080) port for the ambrose tool to listen on.</li>
 *   <li><pre>ambrose.post.script.sleep.seconds</pre> number of seconds to keep the VM running after
 *   the script is complete. This is useful to keep Ambrose up once the job is done.</li>
 * </ul>
 * </P>
 * @author xplenty
 */
public class HttpAmbrosePigProgressNotificationListener extends AmbrosePigProgressNotificationListener {

	  public HttpAmbrosePigProgressNotificationListener() {
	    super(new HttpStatsWriteService());
	  }

	  public HttpAmbrosePigProgressNotificationListener(String url) {
	    super(new HttpStatsWriteService(url));
	  }
}

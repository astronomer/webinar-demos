REPLACEMENT_LIST = [
    ("Other Airflow 2 version (please specify below)", ""),
    ('### If "Other Airflow 2 version" selected, which one?', ""),
    ("### What happened?", ""),
    ("### What you think should happen instead?", ""),
    ("### How to reproduce", ""),
    ("### Versions of Apache Airflow Providers", ""),
    ("### Deployment", ""),
    (
        """Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->""",
        "",
    ),
    (
        """<!--
Thank you for contributing! Please make sure that your code changes
are covered with tests. And in case of new features or big changes
remember to adjust the documentation.

Feel free to ping committers for the review!

In case of an existing issue, reference it using one of the following:

closes: #ISSUE
related: #ISSUE

How to write a good git commit message:
http://chris.beams.io/posts/git-commit/
-->""",
        "",
    ),
    ("<!-- Please keep an empty line above the dashes. -->", ""),
    (
        """### Code of Conduct

- [X] I agree to follow this project's [Code of Conduct](https://github.com/apache/airflow/blob/main/CODE_OF_CONDUCT.md)""",
        "",
    ),
    (
        """**^ Add meaningful description above**
Read the **[Pull Request Guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-guidelines)** for more information.
In case of fundamental code changes, an Airflow Improvement Proposal ([AIP](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)) is needed.
In case of a new dependency, check compliance with the [ASF 3rd Party License Policy](https://www.apache.org/legal/resolved.html#category-x).
In case of backwards incompatible changes please leave a note in a newsfragment file, named `{pr_number}.significant.rst` or `{issue_number}.significant.rst`, in [newsfragments](https://github.com/apache/airflow/tree/main/newsfragments).""",
        "",
    ),
    (
        '<!--\r\n Licensed to the Apache Software Foundation (ASF) under one\r\n or more contributor license agreements. See the NOTICE file\r\n distributed with this work for additional information\r\n regarding copyright ownership. The ASF licenses this file\r\n to you under the Apache License, Version 2.0 (the\r\n "License"); you may not use this file except in compliance\r\n with the License. You may obtain a copy of the License at\r\n\r\n http://www.apache.org/licenses/LICENSE-2.0\r\n\r\n Unless required by applicable law or agreed to in writing,\r\n software distributed under the License is distributed on an\r\n "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\r\n KIND, either express or implied. See the License for the\r\n specific language governing permissions and limitations\r\n under the License.\r\n -->\r\n\r\n<!--\r\nThank you for contributing! Please make sure that your code changes\r\nare covered with tests. And in case of new features or big changes\r\nremember to adjust the documentation.\r\n\r\nFeel free to ping committers for the review!\r\n\r\nIn case of an existing issue, reference it using one of the following:\r\n\r\ncloses: #ISSUE\r\nrelated: #ISSUE\r\n\r\nHow to write a good git commit message:\r\nhttp://chris.beams.io/posts/git-commit/\r\n-->',
        "",
    ),
]

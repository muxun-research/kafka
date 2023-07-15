/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.image.node;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.metadata.authorizer.StandardAcl;

import java.util.ArrayList;
import java.util.Collection;


public class AclsImageByIdNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "byId";

    /**
     * The ACLs image.
     */
    private final AclsImage image;

    public AclsImageByIdNode(AclsImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        for (Uuid uuid : image.acls().keySet()) {
            childNames.add(uuid.toString());
        }
        return childNames;
    }

    @Override
    public MetadataNode child(String name) {
        Uuid uuid;
        try {
            uuid = Uuid.fromString(name);
        } catch (Exception e) {
            return null;
        }
        StandardAcl acl = image.acls().get(uuid);
        if (acl == null)
            return null;
        return new MetadataLeafNode(acl.toString());
    }
}

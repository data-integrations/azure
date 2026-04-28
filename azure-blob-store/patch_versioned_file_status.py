import sys
import os
import struct
import zipfile
import re

def patch_manifest(manifest_bytes):
    manifest = manifest_bytes.decode('utf-8')

    def fix_export(m):
        exports = m.group(1)
        exports = exports.replace('org.apache.hadoop.fs.azurebfs', 'io.cdap.plugin.azure.shaded.azurebfs')
        exports = exports.replace('org.apache.hadoop.fs.azure', 'io.cdap.plugin.azure.shaded.azure')
        return 'Export-Package: ' + exports

    manifest = re.sub(r'Export-Package: (.*?)(?=\n[A-Z][a-zA-Z0-9-]*:|\Z)', fix_export, manifest, flags=re.DOTALL)

    def fix_import(m):
        imports = m.group(1)
        pkgs = [p.strip() for p in imports.split(',')]
        filtered = [p for p in pkgs if not (p.startswith('org.apache.hadoop.fs.azure') or p.startswith('io.cdap.plugin.azure.shaded'))]
        return 'Import-Package: ' + ','.join(filtered)

    manifest = re.sub(r'Import-Package: (.*?)(?=\n[A-Z][a-zA-Z0-9-]*:|\Z)', fix_import, manifest, flags=re.DOTALL)
    return manifest.encode('utf-8')

def patch_jar(jar_path):
    print("Patching VersionedFileStatus and MANIFEST.MF in " + jar_path)
    with zipfile.ZipFile(jar_path, 'r') as z:
        file_map = {name: z.read(name) for name in z.namelist()}

    if 'META-INF/MANIFEST.MF' in file_map:
        file_map['META-INF/MANIFEST.MF'] = patch_manifest(file_map['META-INF/MANIFEST.MF'])
        print("Successfully patched META-INF/MANIFEST.MF!")

    matching_names = [n for n in file_map if n.endswith('VersionedFileStatus.class') and 'AzureBlobFileSystemStore' in n]
    if not matching_names:
        print("Warning: VersionedFileStatus.class not found in " + jar_path)
    else:
        target_name = matching_names[0]
        print("Found target_name: " + target_name)
        data = bytearray(file_map[target_name])

        old_desc = '(JZIJJJLorg/apache/hadoop/fs/permission/FsPermission;Ljava/lang/String;Ljava/lang/String;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/Path;ZZZ)V'
        new_desc = '(JZIJJJLorg/apache/hadoop/fs/permission/FsPermission;Ljava/lang/String;Ljava/lang/String;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/Path;)V'

        old_b = old_desc.encode('utf-8')
        new_b = new_desc.encode('utf-8')

        desc_idx = data.find(old_b)
        if desc_idx > 0:
            data[desc_idx - 2 : desc_idx] = struct.pack('>H', len(new_b))
            data[desc_idx : desc_idx + len(old_b)] = new_b

        old_code_prefix = bytes.fromhex('2a1605150715081609160b092d2b2c01190d15040303b7')
        c_idx = data.find(old_code_prefix)
        if c_idx > 0:
            code_attr_idx = c_idx - 14
            old_attr_name_i, old_attr_len, old_max_s, old_max_l, old_code_len = struct.unpack(
                '>HIHHI', data[code_attr_idx : code_attr_idx + 14]
            )

            cp_bytes = data[c_idx + len(old_code_prefix) : c_idx + len(old_code_prefix) + 2]
            rest_code = data[c_idx + len(old_code_prefix) + 2 : c_idx + old_code_len]
            new_code = bytes.fromhex('2a1605150715081609160b092d2b2c01190db7') + cp_bytes + rest_code

            old_attr_end = code_attr_idx + 6 + old_attr_len
            new_attr_body = struct.pack('>HHI', old_max_s, old_max_l, len(new_code)) + new_code + bytes.fromhex('0000 0000')
            new_attr_len = len(new_attr_body)

            data[code_attr_idx + 2 : code_attr_idx + 6] = struct.pack('>I', new_attr_len)
            data[code_attr_idx + 6 : old_attr_end] = new_attr_body

        file_map[target_name] = bytes(data)
        print("Successfully patched VersionedFileStatus!")

    with zipfile.ZipFile(jar_path, 'w', zipfile.ZIP_DEFLATED) as zout:
        for name, content in file_map.items():
            zout.writestr(name, content)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: patch_versioned_file_status.py <jar_path>")
        sys.exit(1)
    patch_jar(sys.argv[1])

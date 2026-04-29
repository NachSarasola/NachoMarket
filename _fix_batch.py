import os
target = 'src/polymarket'
for f in os.listdir(target):
    if f.startswith('client'):
        real_path = os.path.join(target, f)
        print(f'Found: {repr(real_path)}')
        
        data = open(real_path, 'r', encoding='utf-8').read()
        old = 'batch_result = self._client.post_orders(orders_with_type)'
        new = 'batch_result = self._client.post_orders(orders_with_type, post_only=True)'
        if old in data:
            data = data.replace(old, new)
            open(real_path, 'w', encoding='utf-8').write(data)
            print('Fixed!')
        else:
            lines = data.splitlines()
            for i, line in enumerate(lines):
                if 'post_orders' in line and 'orders_with_type' in line:
                    print(f'Line {i+1}: {repr(line)}')
                    new_line = 'batch_result = self._client.post_orders(orders_with_type, post_only=True)'
                    lines[i] = new_line
                    data = '\n'.join(lines)
                    open(real_path, 'w', encoding='utf-8').write(data)
                    print(f'Fixed line {i+1}')
                    break
        break
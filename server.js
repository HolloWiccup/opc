const opcua = require("node-opcua");
const ModbusRTU = require("modbus-serial");
const express = require("express");
const path = require("path");
const fs = require("fs");
const { SerialPort } = require('serialport');

// Конфигурация
const OPC_UA_PORT = 52000;
const WEB_PORT = 3000;
const CONFIG_FILE = 'devices.json';

// Создаем Express сервер для веб-интерфейса
const webApp = express();
webApp.use(express.json());
webApp.use(express.static('public'));

// Создаем OPC UA сервер
const server = new opcua.OPCUAServer({
    port: OPC_UA_PORT,
    resourcePath: "/UA/MyServer",
    buildInfo: {
        productName: "Modbus-OPC-UA-Bridge",
        buildNumber: "1.0.0"
    }
});

let devices = [];
let modbusClients = new Map();
let opcuaVariables = new Map();

// Загрузка конфигурации устройств
function loadDevicesConfig() {
    try {
        if (fs.existsSync(CONFIG_FILE)) {
            const data = fs.readFileSync(CONFIG_FILE, 'utf8');
            devices = JSON.parse(data);
            console.log(`Загружено ${devices.length} устройств из конфигурации`);
        }
    } catch (error) {
        console.error("Ошибка загрузки конфигурации:", error);
        devices = [];
    }
}

// Сохранение конфигурации устройств
function saveDevicesConfig() {
    try {
        fs.writeFileSync(CONFIG_FILE, JSON.stringify(devices, null, 2));
        console.log("Конфигурация устройств сохранена");
    } catch (error) {
        console.error("Ошибка сохранения конфигурации:", error);
    }
}

// API маршруты
webApp.get('/api/devices', (req, res) => {
    res.json(devices);
});

webApp.post('/api/devices', (req, res) => {
    try {
        const newDevice = req.body;
        
        // Валидация
        if (!newDevice.name || !newDevice.type || !newDevice.tags || !Array.isArray(newDevice.tags)) {
            return res.status(400).json({ error: "Неверные данные устройства" });
        }

        // Генерируем ID если нет
        if (!newDevice.id) {
            newDevice.id = Date.now().toString();
        }

        devices.push(newDevice);
        saveDevicesConfig();
        
        // Инициализируем новое устройство
        initializeDevice(newDevice);
        
        res.json({ success: true, device: newDevice });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

webApp.delete('/api/devices/:id', (req, res) => {
    try {
        const deviceId = req.params.id;
        const index = devices.findIndex(d => d.id === deviceId);
        
        if (index === -1) {
            return res.status(404).json({ error: "Устройство не найдено" });
        }

        // Удаляем OPC UA переменные
        removeDeviceVariables(deviceId);
        
        // Закрываем Modbus соединение
        const client = modbusClients.get(deviceId);
        if (client) {
            client.close().catch(() => {});
            modbusClients.delete(deviceId);
        }

        devices.splice(index, 1);
        saveDevicesConfig();
        
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

webApp.get('/api/values', (req, res) => {
    const values = {};
    devices.forEach(device => {
        values[device.id] = {
            name: device.name,
            tags: {}
        };
        device.tags.forEach(tag => {
            values[device.id].tags[tag.name] = {
                value: tag.currentValue || 0,
                writable: isTagWritable(tag.registerType)
            };
        });
    });
    res.json(values);
});

// Новый endpoint для записи значений
webApp.post('/api/write', async (req, res) => {
    try {
        const { deviceId, tagName, value } = req.body;
        
        if (!deviceId || !tagName || value === undefined) {
            return res.status(400).json({ error: "Неверные параметры" });
        }

        const device = devices.find(d => d.id === deviceId);
        if (!device) {
            return res.status(404).json({ error: "Устройство не найдено" });
        }

        const tag = device.tags.find(t => t.name === tagName);
        if (!tag) {
            return res.status(404).json({ error: "Тег не найден" });
        }

        if (!isTagWritable(tag.registerType)) {
            return res.status(400).json({ error: "Этот тег доступен только для чтения" });
        }

        // Записываем значение в устройство
        const success = await writeTagValue(device, tag, parseFloat(value));
        
        if (success) {
            // Обновляем значение в OPC UA
            const variable = opcuaVariables.get(deviceId)?.get(tagName);
            if (variable) {
                variable.setValueFromSource(new opcua.Variant({
                    dataType: getOPCUADataTypeCode(tag.dataType),
                    value: tag.currentValue
                }));
            }
            
            res.json({ success: true, value: tag.currentValue });
        } else {
            res.status(500).json({ error: "Ошибка записи в устройство" });
        }

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

webApp.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

webApp.get('/add-device', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'add-device.html'));
});

async function main() {
    try {
        // Загружаем конфигурацию
        loadDevicesConfig();

        // Запускаем веб-сервер
        webApp.listen(WEB_PORT, () => {
            console.log(`Веб-интерфейс доступен по адресу: http://localhost:${WEB_PORT}`);
        });

        // Инициализация OPC UA сервера
        await server.initialize();
        console.log("OPC UA сервер инициализирован");

        // Создаем адресное пространство
        const addressSpace = server.engine.addressSpace;
        const namespace = addressSpace.getOwnNamespace();

        // Создаем корневую папку для устройств
        const devicesFolder = namespace.addFolder(addressSpace.rootFolder.objects, {
            browseName: "ModbusDevices"
        });

        // Инициализируем все устройства из конфигурации
        devices.forEach(device => {
            initializeOPCUADevice(device, namespace, devicesFolder);
            initializeModbusClient(device);
        });

        console.log("Устройства инициализированы");

        // Запускаем сервер
        await server.start();
        console.log(`OPC UA сервер запущен на порту ${OPC_UA_PORT}`);
        console.log(`Endpoint URL: ${server.endpoints[0].endpointDescriptions()[0].endpointUrl}`);

        // Запускаем опрос всех устройств
        startAllDevicesPolling();

    } catch (error) {
        console.error("Ошибка:", error);
    }
}

function initializeDevice(device) {
    const addressSpace = server.engine.addressSpace;
    const namespace = addressSpace.getOwnNamespace();
    const devicesFolder = namespace.addFolder(addressSpace.rootFolder.objects, {
        browseName: "ModbusDevices"
    });

    initializeOPCUADevice(device, namespace, devicesFolder);
    initializeModbusClient(device);
    startDevicePolling(device);
}

function initializeOPCUADevice(device, namespace, parentFolder) {
    // Создаем объект устройства
    const deviceObject = namespace.addObject({
        organizedBy: parentFolder,
        browseName: device.name
    });

    // Создаем переменные для каждого тега
    device.tags.forEach(tag => {
        const isWritable = isTagWritable(tag.registerType);
        
        const variable = namespace.addVariable({
            componentOf: deviceObject,
            browseName: tag.name,
            nodeId: `s=${device.id}_${tag.name}`,
            dataType: getOPCUADataType(tag.dataType),
            value: {
                get: () => new opcua.Variant({
                    dataType: getOPCUADataTypeCode(tag.dataType),
                    value: tag.currentValue || 0
                }),
                set: isWritable ? (variant) => {
                    const newValue = variant.value;
                    console.log(`OPC UA запись: ${tag.name} = ${newValue}`);
                    writeTagValue(device, tag, newValue).then(success => {
                        if (success) {
                            console.log(`Значение ${tag.name} успешно записано`);
                        }
                    });
                    return opcua.StatusCodes.Good;
                } : undefined
            },
            minimumSamplingInterval: device.pollInterval || 1000,
            accessLevel: isWritable ? 
                opcua.makeAccessLevelFlag("CurrentRead | CurrentWrite") : 
                opcua.makeAccessLevelFlag("CurrentRead")
        });

        // Сохраняем ссылку на переменную
        if (!opcuaVariables.has(device.id)) {
            opcuaVariables.set(device.id, new Map());
        }
        opcuaVariables.get(device.id).set(tag.name, variable);
    });
}

function initializeModbusClient(device) {
    const client = new ModbusRTU();
    
    client.on("error", (error) => {
        console.error(`Modbus ошибка устройства ${device.name}:`, error.message);
        device.connected = false;
    });

    client.on("close", () => {
        console.log(`Modbus соединение устройства ${device.name} закрыто`);
        device.connected = false;
    });

    modbusClients.set(device.id, client);
}

async function connectToDevice(device) {
    const client = modbusClients.get(device.id);
    if (!client) return false;

    if (device.connected) return true;

    try {
        if (device.type === 'tcp') {
            await client.connectTCP(device.address, { port: device.port || 502 });
        } else if (device.type === 'rtu') {
            await client.connectRTUBuffered(device.address, {
                baudRate: device.baudRate || 9600,
                dataBits: 8,
                stopBits: 1,
                parity: 'none'
            });
        }
        
        client.setID(device.deviceId || 1);
        device.connected = true;
        console.log(`Подключено к устройству ${device.name}`);
        return true;
    } catch (error) {
        console.error(`Ошибка подключения к устройству ${device.name}:`, error.message);
        device.connected = false;
        return false;
    }
}

async function readDeviceData(device) {
    const client = modbusClients.get(device.id);
    if (!client) return;

    if (!device.connected) {
        const connected = await connectToDevice(device);
        if (!connected) return;
    }

    for (const tag of device.tags) {
        try {
            let data;
            if (tag.registerType === 'holding') {
                data = await client.readHoldingRegisters(tag.address, getRegisterCount(tag.dataType));
            } else if (tag.registerType === 'input') {
                data = await client.readInputRegisters(tag.address, getRegisterCount(tag.dataType));
            } else if (tag.registerType === 'coil') {
                data = await client.readCoils(tag.address, 1);
            } else if (tag.registerType === 'discrete') {
                data = await client.readDiscreteInputs(tag.address, 1);
            }

            if (data && data.data) {
                const value = convertModbusData(data.data, tag.dataType);
                tag.currentValue = value;
                
                // Обновляем OPC UA переменную
                const variable = opcuaVariables.get(device.id)?.get(tag.name);
                if (variable) {
                    variable.setValueFromSource(new opcua.Variant({
                        dataType: getOPCUADataTypeCode(tag.dataType),
                        value: value
                    }));
                }

                console.log(`Устройство ${device.name}, тег ${tag.name}: ${value}`);
            }
        } catch (error) {
            console.error(`Ошибка чтения тега ${tag.name} устройства ${device.name}:`, error.message);
            device.connected = false;
            try {
                await client.close();
            } catch (closeError) {}
        }
    }
}

// Новая функция для записи значений
async function writeTagValue(device, tag, value) {
    const client = modbusClients.get(device.id);
    if (!client) return false;

    if (!device.connected) {
        const connected = await connectToDevice(device);
        if (!connected) return false;
    }

    try {
        // Конвертируем значение в формат Modbus
        const modbusValue = convertToModbusFormat(value, tag.dataType);
        
        if (tag.registerType === 'holding') {
            if (tag.dataType === 'float' || tag.dataType === 'int32' || tag.dataType === 'uint32') {
                // Для 32-битных значений пишем 2 регистра
                const buffer = Buffer.alloc(4);
                if (tag.dataType === 'float') {
                    buffer.writeFloatBE(value, 0);
                } else {
                    buffer.writeUInt32BE(value, 0);
                }
                await client.writeRegisters(tag.address, [
                    buffer.readUInt16BE(0),
                    buffer.readUInt16BE(2)
                ]);
            } else {
                // Для 16-битных значений пишем один регистр
                await client.writeRegister(tag.address, modbusValue);
            }
        } else if (tag.registerType === 'coil') {
            await client.writeCoil(tag.address, Boolean(value));
        }

        // Обновляем текущее значение
        tag.currentValue = value;
        console.log(`Записано значение: ${tag.name} = ${value}`);
        
        return true;
    } catch (error) {
        console.error(`Ошибка записи тега ${tag.name}:`, error.message);
        device.connected = false;
        try {
            await client.close();
        } catch (closeError) {}
        return false;
    }
}

function convertToModbusFormat(value, dataType) {
    switch (dataType) {
        case 'float':
        case 'int32':
        case 'uint32':
            return value; // Обрабатывается отдельно
        case 'int16':
            return value < 0 ? value + 65536 : value;
        case 'uint16':
            return value;
        case 'boolean':
            return Boolean(value) ? 1 : 0;
        default:
            return value;
    }
}

function isTagWritable(registerType) {
    return registerType === 'holding' || registerType === 'coil';
}

function getRegisterCount(dataType) {
    switch (dataType) {
        case 'float': return 2;
        case 'int32': return 2;
        case 'uint32': return 2;
        default: return 1;
    }
}

function convertModbusData(data, dataType) {
    switch (dataType) {
        case 'float':
            const buffer = Buffer.alloc(4);
            buffer.writeUInt16BE(data[0], 0);
            buffer.writeUInt16BE(data[1], 2);
            return buffer.readFloatBE(0);
        case 'int32':
            return (data[0] << 16) + data[1];
        case 'uint32':
            return (data[0] << 16) + data[1];
        case 'int16':
            return data[0] > 32767 ? data[0] - 65536 : data[0];
        case 'uint16':
            return data[0];
        case 'boolean':
            return Boolean(data[0]);
        default:
            return data[0];
    }
}

function getOPCUADataType(dataType) {
    const map = {
        'float': 'Float',
        'int32': 'Int32',
        'uint32': 'UInt32',
        'int16': 'Int16',
        'uint16': 'UInt16',
        'boolean': 'Boolean'
    };
    return map[dataType] || 'UInt16';
}

function getOPCUADataTypeCode(dataType) {
    const map = {
        'float': opcua.DataType.Float,
        'int32': opcua.DataType.Int32,
        'uint32': opcua.DataType.UInt32,
        'int16': opcua.DataType.Int16,
        'uint16': opcua.DataType.UInt16,
        'boolean': opcua.DataType.Boolean
    };
    return map[dataType] || opcua.DataType.UInt16;
}

function startAllDevicesPolling() {
    devices.forEach(device => {
        startDevicePolling(device);
    });
}

function startDevicePolling(device) {
    setInterval(() => {
        readDeviceData(device);
    }, device.pollInterval || 2000);
}

function removeDeviceVariables(deviceId) {
    const variables = opcuaVariables.get(deviceId);
    if (variables) {
        variables.forEach(variable => {
            // Удаляем переменную из адресного пространства
            variable.dispose();
        });
        opcuaVariables.delete(deviceId);
    }
}

// Обработка завершения
process.on("SIGINT", async () => {
    console.log("Остановка сервера...");
    
    // Закрываем все Modbus соединения
    for (const [deviceId, client] of modbusClients) {
        try {
            await client.close();
            console.log(`Modbus соединение устройства ${deviceId} закрыто`);
        } catch (error) {
            console.error(`Ошибка при закрытии Modbus соединения ${deviceId}:`, error.message);
        }
    }
    
    // Останавливаем OPC UA сервер
    await server.shutdown();
    console.log("Сервер остановлен");
    process.exit(0);
});

process.on("unhandledRejection", (error) => {
    console.error("Необработанная ошибка:", error);
});

main().catch(error => {
    console.error("Критическая ошибка при запуске:", error);
    process.exit(1);
});

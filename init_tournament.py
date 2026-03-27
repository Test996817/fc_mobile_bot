"""
Initialize Universe of Heroes Tournament
Add all 64 players to the database
"""

from database import Database

PLAYERS = {
    'Группа A': [
        'Tribalchief_OTC1', 'shtorm10', 'TrunksFC', 'Vadim260401', 
        'Danis1985_RM', 'PrincepsInferni_Flavoculus', 'yura192883', 'Annndreiii',
        'vtrrgyg', 'GodFatherVSA', 'NotEterna1', 'legolas422', 'Vit88', 
        'visshera', 'YarcheeVsa', 'bad_606'
    ],
    'Группа B': [
        'kaistroden', 'prostotip550', 'Andrey_17A', 'joeyhuichan',
        'Dr_Wh11te', 'Yur4ik_1987', 'FcTaganrog', 'mihasik400',
        'tricky938', 'ded1747n', 'shaut', 'likshonn',
        'arnurchik17', 'alexmagenta', 'platonm09', 'Anatolyveniva4'
    ],
    'Группа C': [
        'sp1r1tVSA', 'Arm032', 'KesarTM', 'arrowsgang',
        'StrongMannVSA', 'reconquistaR9', 'zaali_916', 'dopolnitelb',
        'Willamsfire', 'korzhiman98', 'prostomaksx', 'NNovitskiy',
        'freezy_66', 'gatalskiy58', 'onvamnemihaaa', 'Serghe1KO'
    ],
    'Группа D': [
        'RomaS93', 'Yerb0ll', 'artsmilex', 'Moes1k',
        'Sergo1233', 'blancos_15', 'Azat72172', 'Vital0587',
        'ShakhtarDonetsk56', 'ConstantinoXIII', 'shomashoma98', 'Frost_5454',
        'KDI_BY', 'S_G_A_MVP', 'karbon0_0', 'Aleksei19811'
    ]
}


def init_tournament():
    db = Database()
    print("Initializing Universe of Heroes tournament...")
    
    total_added = 0
    for group_name, players in PLAYERS.items():
        print(f"\n{group_name}:")
        for nick in players:
            if db.add_player(nick):
                print(f"  + {nick}")
                total_added += 1
            else:
                print(f"  ! {nick} (already exists or error)")
    
    print(f"\nTotal players added: {total_added}/64")
    print("\nInitialization complete!")


if __name__ == "__main__":
    init_tournament()

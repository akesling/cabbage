pub mod proxy;

use anyhow::anyhow;

pub static HAIKUS: [[&str; 3]; 10] = [
    [
        "Cabbage speaks in shards",
        "Keys refract in veiled silence",
        "Reflected values",
    ],
    [
        "Across distant glass",
        "The echoes of gets and puts",
        "Drift through wire and fog",
    ],
    [
        "A shimmer of keys",
        "Projected from some dark place",
        "Returns not their source",
    ],
    [
        "Under leafy lens",
        "Cabbage splits the call in twain",
        "One here, one beyond",
    ],
    [
        "Put it through the pane",
        "It lands in unseen gardens",
        "Read back through the mist",
    ],
    [
        "Time is not so fixed",
        "Mirrored states in transit shift",
        "The value was... this?",
    ],
    [
        "Behind glassy veil",
        "A cabbage curls around truth",
        "Decoding the void",
    ],
    [
        "Call and it may hear",
        "Though the mirror gives no sign",
        "It stores what it wills",
    ],
    [
        "The proxy stands still",
        "Yet ripples distort the depth",
        "One key, many truths",
    ],
    [
        "Cabbage knows your keys",
        "But never shows its own face",
        "Just reflections, stored",
    ],
];

/// Print a random project-related haiku
pub fn print_haiku(print_all: bool) -> anyhow::Result<()> {
    use rand::seq::SliceRandom as _;

    if print_all {
        for h in HAIKUS {
            println!("{}", h.join(" : "))
        }
    } else {
        let mut rng = rand::thread_rng();
        println!(
            "{}",
            HAIKUS
                .choose(&mut rng)
                .ok_or(anyhow!("at least one haiku"))?
                .join("\n")
        )
    }
    Ok(())
}
